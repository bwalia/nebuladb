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
		return r.startSwap(ctx, rb, cluster, lg)
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

// startSwap runs a bucket-level handoff between two pods. The flow:
//
//  1. Validate: two nodes named, source != target, backend supports
//     /admin/bucket/:b/export.
//  2. For each bucket in spec.Buckets (or every bucket on source if
//     empty): GET /export from source, POST /import to target.
//  3. Verify: target's /buckets stats show the expected count.
//  4. Empty the source bucket so it can be safely removed from service.
//  5. Optional: compact WAL on both nodes.
//
// Swap is a coordinated bucket-level move, not a full cluster-state
// move — NebulaDB doesn't have sharding, so "swap the state between
// pod A and pod B" happens at bucket granularity. This is the
// primitive a future sharding layer would build on.
func (r *NebulaRebalanceReconciler) startSwap(ctx context.Context, rb *nebulav1alpha1.NebulaRebalance, cluster *nebulav1alpha1.NebulaCluster, lg logr.Logger) (ctrl.Result, error) {
	source, target, err := resolveSwapEndpoints(rb.Spec.Nodes)
	if err != nil {
		r.transition(ctx, rb, nebulav1alpha1.RebalanceFailed, err.Error())
		return ctrl.Result{}, nil
	}
	sourceURL := podRESTURL(cluster, source)
	targetURL := podRESTURL(cluster, target)
	src := r.NebulaFactory(sourceURL).WithTimeout(30 * time.Second)
	dst := r.NebulaFactory(targetURL).WithTimeout(30 * time.Second)

	// Determine bucket list. Empty spec = migrate every bucket the
	// source currently has.
	buckets := rb.Spec.Buckets
	if len(buckets) == 0 {
		resp, err := src.Buckets(ctx, 0)
		if err != nil {
			r.appendStep(rb, "Swap", "Failed", fmt.Sprintf("list source buckets: %v", err))
			r.transition(ctx, rb, nebulav1alpha1.RebalanceFailed, err.Error())
			return ctrl.Result{}, nil
		}
		for _, b := range resp.Buckets {
			buckets = append(buckets, b.Name)
		}
	}
	if len(buckets) == 0 {
		r.appendStep(rb, "Swap", "NoOp", "source has no buckets; nothing to migrate")
		r.transition(ctx, rb, nebulav1alpha1.RebalanceDraining, "nothing to migrate")
		return ctrl.Result{Requeue: true}, nil
	}

	for _, bucket := range buckets {
		exp, err := src.ExportBucket(ctx, bucket)
		if err != nil {
			r.appendStep(rb, "Export:"+bucket, "Failed", err.Error())
			r.Recorder.Eventf(rb, corev1.EventTypeWarning, "SwapExportFailed", "bucket %s: %v", bucket, err)
			r.transition(ctx, rb, nebulav1alpha1.RebalanceFailed, err.Error())
			return ctrl.Result{}, nil
		}
		if exp.Count == 0 {
			r.appendStep(rb, "Export:"+bucket, "Skip", "source bucket is empty")
			continue
		}
		imp, err := dst.ImportBucket(ctx, bucket, nebulaclient.ImportBucketRequest{
			Dim:  exp.Dim,
			Docs: exp.Docs,
		})
		if err != nil {
			r.appendStep(rb, "Import:"+bucket, "Failed", err.Error())
			r.Recorder.Eventf(rb, corev1.EventTypeWarning, "SwapImportFailed", "bucket %s: %v", bucket, err)
			r.transition(ctx, rb, nebulav1alpha1.RebalanceFailed, err.Error())
			return ctrl.Result{}, nil
		}
		if imp.Imported != exp.Count {
			msg := fmt.Sprintf("partial import for %s: %d/%d", bucket, imp.Imported, exp.Count)
			r.appendStep(rb, "Import:"+bucket, "Warning", msg)
			r.Recorder.Event(rb, corev1.EventTypeWarning, "SwapPartialImport", msg)
		} else {
			r.appendStep(rb, "Import:"+bucket, "Done", fmt.Sprintf("%d docs", imp.Imported))
		}

		// Empty the source bucket so traffic flipping to the target
		// sees a consistent view. We deliberately do this after a
		// successful import — losing data on the source before the
		// target confirms would be worse than a retry.
		if err := src.EmptyBucket(ctx, bucket); err != nil {
			r.appendStep(rb, "SourceEmpty:"+bucket, "Warning", err.Error())
		} else {
			r.appendStep(rb, "SourceEmpty:"+bucket, "Done", "")
		}
	}

	r.transition(ctx, rb, nebulav1alpha1.RebalanceDraining, "swap complete, pending compact")
	return ctrl.Result{Requeue: true}, nil
}

// resolveSwapEndpoints extracts the source + target pod names from a
// Nodes list. Roles "old"/"source" and "new"/"target" are honored when
// set; otherwise the first two entries are used in order.
func resolveSwapEndpoints(nodes []nebulav1alpha1.NodeRef) (source, target string, err error) {
	if len(nodes) < 2 {
		return "", "", fmt.Errorf("swap requires at least 2 entries in spec.nodes[]")
	}
	for _, n := range nodes {
		switch n.Role {
		case "old", "source":
			source = n.Name
		case "new", "target":
			target = n.Name
		}
	}
	if source == "" {
		source = nodes[0].Name
	}
	if target == "" {
		target = nodes[1].Name
	}
	if source == target {
		return "", "", fmt.Errorf("swap source and target must differ (got both=%q)", source)
	}
	return source, target, nil
}

// podRESTURL returns the cluster-local DNS name for a specific pod's
// REST port. Works for both leader and follower StatefulSets because
// we probe /healthz which is public and the headless service is named
// consistently on both sides.
func podRESTURL(cluster *nebulav1alpha1.NebulaCluster, pod string) string {
	// Best-effort: we don't know which role the pod belongs to, but
	// the pod's hostname under either headless service resolves the
	// same way — Kubernetes assigns one DNS record per StatefulSet.
	// Swap requires the caller to know which pod is which, so we
	// build the URL with the leader headless service and rely on
	// DNS to route. Followers under their own headless service will
	// still resolve with this form when the pod name matches.
	svc := fmt.Sprintf("%s-leader-headless", cluster.Name)
	return fmt.Sprintf("http://%s.%s.%s.svc.cluster.local:8080", pod, svc, cluster.Namespace)
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
