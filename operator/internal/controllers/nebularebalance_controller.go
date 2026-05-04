package controllers

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	nebulav1alpha1 "github.com/bwalia/nebuladb-operator/api/v1alpha1"
	"github.com/bwalia/nebuladb-operator/internal/nebulaclient"
)

// NebulaRebalanceReconciler coordinates one-shot rebalance/failover operations.
//
// State machine:
//
//	Pending        -> snapshot leader (if SnapshotBefore)    -> Preflight
//	Preflight      -> dispatch by Type
//	                    Standard -> update cluster.followers  -> Rebalancing
//	                    Failover -> promote follower          -> Promoting
//	                    Swap     -> NotImplemented            -> terminal
//	Rebalancing    -> wait for cluster follower ready count   -> Draining
//	Promoting      -> patch chosen follower's role env        -> Draining
//	Draining       -> optional compact WAL                    -> Completed
//
// The CR is single-fire: once Completed or Failed, no further work happens.
type NebulaRebalanceReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Recorder      record.EventRecorder
	NebulaFactory func(base string) *nebulaclient.Client
}

// +kubebuilder:rbac:groups=nebula.nebuladb.io,resources=nebularebalances,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nebula.nebuladb.io,resources=nebularebalances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nebula.nebuladb.io,resources=nebularebalances/finalizers,verbs=update

func (r *NebulaRebalanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lg := log.FromContext(ctx).WithValues("nebularebalance", req.NamespacedName)

	var rb nebulav1alpha1.NebulaRebalance
	if err := r.Get(ctx, req.NamespacedName, &rb); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Terminal states: no further reconciliation.
	if rb.Status.Phase == nebulav1alpha1.RebalanceCompleted ||
		rb.Status.Phase == nebulav1alpha1.RebalanceFailed ||
		rb.Status.Phase == nebulav1alpha1.RebalanceNotImplemented {
		return ctrl.Result{}, nil
	}

	// Fetch parent cluster. If missing, wait for user intervention.
	var cluster nebulav1alpha1.NebulaCluster
	if err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: rb.Spec.ClusterRef}, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			r.transition(ctx, &rb, nebulav1alpha1.RebalanceFailed, fmt.Sprintf("cluster %q not found", rb.Spec.ClusterRef))
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !rb.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&rb, FinalizerRebalance) {
			controllerutil.RemoveFinalizer(&rb, FinalizerRebalance)
			return ctrl.Result{}, r.Update(ctx, &rb)
		}
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(&rb, FinalizerRebalance) {
		controllerutil.AddFinalizer(&rb, FinalizerRebalance)
		if err := r.Update(ctx, &rb); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Enforce timeout.
	if rb.Status.StartedAt != nil && rb.Spec.TimeoutSeconds > 0 {
		deadline := rb.Status.StartedAt.Add(time.Duration(rb.Spec.TimeoutSeconds) * time.Second)
		if time.Now().After(deadline) {
			r.transition(ctx, &rb, nebulav1alpha1.RebalanceFailed, "timeout exceeded")
			return ctrl.Result{}, nil
		}
	}

	switch rb.Status.Phase {
	case "", nebulav1alpha1.RebalancePending:
		return r.startRebalance(ctx, &rb, &cluster, lg)
	case nebulav1alpha1.RebalancePreflight:
		return r.dispatch(ctx, &rb, &cluster, lg)
	case nebulav1alpha1.RebalanceRebalancing:
		return r.checkRebalanceProgress(ctx, &rb, &cluster, lg)
	case nebulav1alpha1.RebalancePromoting:
		return r.checkPromoteProgress(ctx, &rb, &cluster, lg)
	case nebulav1alpha1.RebalanceDraining:
		return r.finalize(ctx, &rb, &cluster, lg)
	}
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

// startRebalance: snapshot first (if requested), then move to Preflight.
func (r *NebulaRebalanceReconciler) startRebalance(ctx context.Context, rb *nebulav1alpha1.NebulaRebalance, cluster *nebulav1alpha1.NebulaCluster, lg logr.Logger) (ctrl.Result, error) {
	if rb.Status.StartedAt == nil {
		now := metav1.Now()
		rb.Status.StartedAt = &now
	}
	r.appendStep(rb, "StartRebalance", "InProgress", fmt.Sprintf("type=%s", rb.Spec.Type))

	if rb.Spec.SnapshotBefore {
		leaderURL := fmt.Sprintf("http://%s-leader.%s.svc.cluster.local:8080", cluster.Name, cluster.Namespace)
		nc := r.NebulaFactory(leaderURL).WithTimeout(10 * time.Second)
		if snap, err := nc.Snapshot(ctx); err != nil {
			r.appendStep(rb, "Snapshot", "Failed", err.Error())
			r.Recorder.Eventf(rb, corev1.EventTypeWarning, "SnapshotFailed", "%v", err)
			// Snapshot failure is non-fatal — warn and proceed.
		} else {
			r.appendStep(rb, "Snapshot", "Done", fmt.Sprintf("wal_seq=%d path=%s", snap.WalSeqCaptured, snap.Path))
		}
	}

	r.transition(ctx, rb, nebulav1alpha1.RebalancePreflight, "preflight complete")
	return ctrl.Result{Requeue: true}, nil
}

func (r *NebulaRebalanceReconciler) dispatch(ctx context.Context, rb *nebulav1alpha1.NebulaRebalance, cluster *nebulav1alpha1.NebulaCluster, lg logr.Logger) (ctrl.Result, error) {
	switch rb.Spec.Type {
	case nebulav1alpha1.RebalanceStandard:
		return r.startStandard(ctx, rb, cluster, lg)
	case nebulav1alpha1.RebalanceFailover:
		return r.startFailover(ctx, rb, cluster, lg)
	case nebulav1alpha1.RebalanceSwap:
		// Swap rebalance requires a NebulaDB rebalance engine which doesn't
		// exist yet (see USE_CASES.md:383). Record the intent for future
		// backend work but do not pretend to execute it.
		msg := "Swap rebalance not implemented: NebulaDB has no rebalance engine today. " +
			"See USE_CASES.md. Use Type=Recreate upgrade instead."
		r.appendStep(rb, "Swap", "NotImplemented", msg)
		r.Recorder.Event(rb, corev1.EventTypeWarning, "SwapNotImplemented", msg)
		r.transition(ctx, rb, nebulav1alpha1.RebalanceNotImplemented, msg)
		return ctrl.Result{}, nil
	}
	r.transition(ctx, rb, nebulav1alpha1.RebalanceFailed, fmt.Sprintf("unknown type %q", rb.Spec.Type))
	return ctrl.Result{}, nil
}

// startStandard adjusts the parent cluster's follower count, which the
// NebulaClusterReconciler picks up on its next pass.
func (r *NebulaRebalanceReconciler) startStandard(ctx context.Context, rb *nebulav1alpha1.NebulaRebalance, cluster *nebulav1alpha1.NebulaCluster, lg logr.Logger) (ctrl.Result, error) {
	if rb.Spec.TargetFollowers == nil {
		// Standard rebalance with no target is a no-op drain/compact request.
		r.appendStep(rb, "Rebalance", "NoOp", "no target follower count supplied")
		r.transition(ctx, rb, nebulav1alpha1.RebalanceDraining, "skipping topology change")
		return ctrl.Result{Requeue: true}, nil
	}

	target := *rb.Spec.TargetFollowers
	before := cluster.Spec.Replication.Followers
	if before == target && cluster.Spec.Replication.Enabled {
		r.appendStep(rb, "Rebalance", "NoOp", fmt.Sprintf("already at %d followers", target))
		r.transition(ctx, rb, nebulav1alpha1.RebalanceDraining, "topology already matches")
		return ctrl.Result{Requeue: true}, nil
	}

	latest := cluster.DeepCopy()
	latest.Spec.Replication.Enabled = target > 0
	latest.Spec.Replication.Followers = target
	if err := r.Patch(ctx, latest, client.MergeFrom(cluster)); err != nil {
		r.appendStep(rb, "Rebalance", "Failed", err.Error())
		r.transition(ctx, rb, nebulav1alpha1.RebalanceFailed, err.Error())
		return ctrl.Result{}, nil
	}
	r.appendStep(rb, "Rebalance", "InProgress", fmt.Sprintf("followers %d -> %d", before, target))
	r.transition(ctx, rb, nebulav1alpha1.RebalanceRebalancing, "cluster patched")
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *NebulaRebalanceReconciler) startFailover(ctx context.Context, rb *nebulav1alpha1.NebulaRebalance, cluster *nebulav1alpha1.NebulaCluster, lg logr.Logger) (ctrl.Result, error) {
	if len(rb.Spec.Nodes) == 0 {
		r.transition(ctx, rb, nebulav1alpha1.RebalanceFailed, "failover requires spec.nodes[] to name failed leader + promotion target")
		return ctrl.Result{}, nil
	}
	r.appendStep(rb, "Failover", "InProgress", "transitioning to Promoting")
	r.transition(ctx, rb, nebulav1alpha1.RebalancePromoting, "promoting replacement")
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *NebulaRebalanceReconciler) checkRebalanceProgress(ctx context.Context, rb *nebulav1alpha1.NebulaRebalance, cluster *nebulav1alpha1.NebulaCluster, lg logr.Logger) (ctrl.Result, error) {
	// Progress == follower readiness count. Cheap + good enough.
	ready := cluster.Status.ReadyReplicas
	desired := cluster.Status.DesiredReplicas
	if desired > 0 {
		rb.Status.ProgressPct = int32((ready * 100) / desired)
	}
	if ready >= desired {
		r.appendStep(rb, "Rebalance", "Done", fmt.Sprintf("%d/%d ready", ready, desired))
		r.transition(ctx, rb, nebulav1alpha1.RebalanceDraining, "cluster fully ready")
		return ctrl.Result{Requeue: true}, nil
	}
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

func (r *NebulaRebalanceReconciler) checkPromoteProgress(ctx context.Context, rb *nebulav1alpha1.NebulaRebalance, cluster *nebulav1alpha1.NebulaCluster, lg logr.Logger) (ctrl.Result, error) {
	// Role-flipping requires patching the follower STS env. Scope: this
	// operator version models promotion as "log the intent, require human
	// approval to edit the NebulaCluster spec.regions[].roles map in a
	// future release". For now, record the plan and move on.
	r.appendStep(rb, "Promote", "Planned", "role patching not yet automated; operator to flip NEBULA_NODE_ROLE manually")
	r.Recorder.Eventf(rb, corev1.EventTypeWarning, "PromotionManual", "manual follower promotion required for nodes=%v", rb.Spec.Nodes)
	r.transition(ctx, rb, nebulav1alpha1.RebalanceDraining, "awaiting manual role flip")
	return ctrl.Result{Requeue: true}, nil
}

func (r *NebulaRebalanceReconciler) finalize(ctx context.Context, rb *nebulav1alpha1.NebulaRebalance, cluster *nebulav1alpha1.NebulaCluster, lg logr.Logger) (ctrl.Result, error) {
	if rb.Spec.CompactAfter {
		leaderURL := fmt.Sprintf("http://%s-leader.%s.svc.cluster.local:8080", cluster.Name, cluster.Namespace)
		nc := r.NebulaFactory(leaderURL).WithTimeout(10 * time.Second)
		if err := nc.CompactWAL(ctx); err != nil {
			r.appendStep(rb, "CompactWAL", "Warning", err.Error())
		} else {
			r.appendStep(rb, "CompactWAL", "Done", "")
		}
	}
	now := metav1.Now()
	rb.Status.CompletedAt = &now
	rb.Status.ProgressPct = 100
	r.transition(ctx, rb, nebulav1alpha1.RebalanceCompleted, "rebalance complete")
	return ctrl.Result{}, nil
}

// transition patches the CR status to move phases forward.
func (r *NebulaRebalanceReconciler) transition(ctx context.Context, rb *nebulav1alpha1.NebulaRebalance, phase nebulav1alpha1.RebalancePhase, msg string) {
	latest := rb.DeepCopy()
	latest.Status.Phase = phase
	latest.Status.Message = msg
	latest.Status.ObservedGeneration = rb.Generation
	cond := metav1.Condition{Type: "Progressing", Status: metav1.ConditionTrue, Reason: string(phase), Message: msg}
	if phase == nebulav1alpha1.RebalanceCompleted {
		cond = metav1.Condition{Type: "Progressing", Status: metav1.ConditionFalse, Reason: "Completed", Message: msg}
	}
	setCondition(&latest.Status.Conditions, cond)
	_ = r.Status().Patch(ctx, latest, client.MergeFrom(rb))
	*rb = *latest
}

func (r *NebulaRebalanceReconciler) appendStep(rb *nebulav1alpha1.NebulaRebalance, name, state, msg string) {
	now := metav1.Now()
	step := nebulav1alpha1.RebalanceStep{
		Name: name, State: state, Message: msg,
		StartedAt: &now,
	}
	rb.Status.Steps = append(rb.Status.Steps, step)
}

func (r *NebulaRebalanceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nebulav1alpha1.NebulaRebalance{}).
		Complete(r)
}
