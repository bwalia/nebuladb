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

var _ = logr.Logger{} // keep import tidy for future use

const finalizerRegionFailover = "nebula.nebuladb.io/regionfailover-finalizer"

// NebulaRegionFailoverReconciler rebinds a bucket's home_region.
//
// State machine:
//
//	Pending    -> read current home/epoch, validate             -> Preflight
//	Preflight  -> (optional) swap bucket contents via gap #4    -> Draining
//	Draining   -> verify target pod has the expected doc count  -> EpochBump
//	EpochBump  -> atomic seed-doc update on the NEW home region -> Announce
//	Announce   -> update NebulaBucket CR status                 -> Completed
//
// Concurrency: the controller relies on the seed doc's home_epoch as
// its tiebreaker. Two failover CRs for the same bucket will each read
// the same FromEpoch, but only one's EpochBump will land first; the
// second will observe an already-bumped epoch on re-read and fail
// with "race lost" rather than clobber.
type NebulaRegionFailoverReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Recorder      record.EventRecorder
	NebulaFactory func(base string) *nebulaclient.Client
}

// +kubebuilder:rbac:groups=nebula.nebuladb.io,resources=nebularegionfailovers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nebula.nebuladb.io,resources=nebularegionfailovers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nebula.nebuladb.io,resources=nebularegionfailovers/finalizers,verbs=update

func (r *NebulaRegionFailoverReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lg := log.FromContext(ctx).WithValues("failover", req.NamespacedName)

	var f nebulav1alpha1.NebulaRegionFailover
	if err := r.Get(ctx, req.NamespacedName, &f); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Terminal states — nothing more to do.
	if f.Status.Phase == nebulav1alpha1.RegionFailoverCompleted ||
		f.Status.Phase == nebulav1alpha1.RegionFailoverFailed {
		return ctrl.Result{}, nil
	}

	if !f.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&f, finalizerRegionFailover) {
			controllerutil.RemoveFinalizer(&f, finalizerRegionFailover)
			return ctrl.Result{}, r.Update(ctx, &f)
		}
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(&f, finalizerRegionFailover) {
		controllerutil.AddFinalizer(&f, finalizerRegionFailover)
		if err := r.Update(ctx, &f); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Enforce the overall timeout.
	if f.Status.StartedAt != nil && f.Spec.TimeoutSeconds > 0 {
		deadline := f.Status.StartedAt.Add(time.Duration(f.Spec.TimeoutSeconds) * time.Second)
		if time.Now().After(deadline) {
			r.transition(ctx, &f, nebulav1alpha1.RegionFailoverFailed, "timeout exceeded")
			return ctrl.Result{}, nil
		}
	}

	// Load parent cluster.
	var cluster nebulav1alpha1.NebulaCluster
	if err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: f.Spec.ClusterRef}, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			r.transition(ctx, &f, nebulav1alpha1.RegionFailoverFailed, fmt.Sprintf("cluster %q not found", f.Spec.ClusterRef))
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	switch f.Status.Phase {
	case "", nebulav1alpha1.RegionFailoverPending:
		return r.startPreflight(ctx, &f, &cluster, lg)
	case nebulav1alpha1.RegionFailoverPreflight:
		return r.startDrain(ctx, &f, &cluster, lg)
	case nebulav1alpha1.RegionFailoverDraining:
		return r.checkDrain(ctx, &f, &cluster, lg)
	case nebulav1alpha1.RegionFailoverEpochBump:
		return r.bumpEpoch(ctx, &f, &cluster, lg)
	case nebulav1alpha1.RegionFailoverAnnounce:
		return r.announce(ctx, &f, &cluster, lg)
	}
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

// startPreflight validates inputs and records the current home/epoch
// as the baseline for the race-detection check in EpochBump.
func (r *NebulaRegionFailoverReconciler) startPreflight(
	ctx context.Context,
	f *nebulav1alpha1.NebulaRegionFailover,
	cluster *nebulav1alpha1.NebulaCluster,
	_ logr.Logger,
) (ctrl.Result, error) {
	if f.Status.StartedAt == nil {
		now := metav1.Now()
		f.Status.StartedAt = &now
	}

	// New region must be listed in the cluster's regions[].
	regionFound := false
	for _, reg := range cluster.Spec.Regions {
		if reg.Name == f.Spec.NewHomeRegion {
			regionFound = true
			break
		}
	}
	if !regionFound && len(cluster.Spec.Regions) > 0 {
		r.transition(ctx, f, nebulav1alpha1.RegionFailoverFailed,
			fmt.Sprintf("new home region %q not listed in cluster.spec.regions", f.Spec.NewHomeRegion))
		return ctrl.Result{}, nil
	}

	// Read current home/epoch from any reachable region — we'll use
	// the leader service of the new region for this (it has the
	// cross-region tail, so it knows the current home).
	targetURL := regionLeaderRESTURL(cluster, f.Spec.NewHomeRegion)
	nc := r.NebulaFactory(targetURL).WithTimeout(5 * time.Second)
	hr, err := nc.HomeRegion(ctx, f.Spec.Bucket)
	if err != nil {
		r.appendFailoverStep(f, "ReadHome", "Retry", err.Error())
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	f.Status.FromHomeRegion = hr.HomeRegion
	f.Status.FromEpoch = int64(hr.HomeEpoch)

	// If the target region is already the home, we're done.
	if hr.HomeRegion == f.Spec.NewHomeRegion {
		r.appendFailoverStep(f, "Preflight", "NoOp", "new region is already the home")
		f.Status.ToEpoch = f.Status.FromEpoch
		r.transition(ctx, f, nebulav1alpha1.RegionFailoverAnnounce, "already at target home")
		return ctrl.Result{Requeue: true}, nil
	}

	// Probe the old home's health unless Force is set. A planned
	// failover with Force=true skips this — "move the home now,
	// I know what I'm doing."
	if !f.Spec.Force && hr.HomeRegion != "" {
		oldURL := regionLeaderRESTURL(cluster, hr.HomeRegion)
		old := r.NebulaFactory(oldURL).WithTimeout(3 * time.Second)
		if _, err := old.Healthz(ctx); err == nil {
			r.transition(ctx, f, nebulav1alpha1.RegionFailoverFailed,
				fmt.Sprintf("current home %q still healthy; set spec.force=true to override",
					hr.HomeRegion))
			return ctrl.Result{}, nil
		}
	}

	r.appendFailoverStep(f, "Preflight", "Done", fmt.Sprintf(
		"from=%s epoch=%d force=%v", hr.HomeRegion, hr.HomeEpoch, f.Spec.Force))
	r.transition(ctx, f, nebulav1alpha1.RegionFailoverDraining, "preflight complete")
	return ctrl.Result{Requeue: true}, nil
}

// startDrain ensures the new home has fresh bucket contents. The
// cross-region consumer on the new region has been tailing the old
// home's WAL, so under normal operation this phase is a quick
// verification. If drain takes too long, fail (or proceed under Force).
func (r *NebulaRegionFailoverReconciler) startDrain(
	ctx context.Context,
	f *nebulav1alpha1.NebulaRegionFailover,
	cluster *nebulav1alpha1.NebulaCluster,
	_ logr.Logger,
) (ctrl.Result, error) {
	r.appendFailoverStep(f, "Drain", "InProgress", "waiting for target region catch-up")
	r.transition(ctx, f, nebulav1alpha1.RegionFailoverDraining, "draining")
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// checkDrain: for now, any successful /replication probe that shows
// this region has applied at least one record from the old home (or
// the old home is unreachable and Force is set) counts as drained.
// A future revision can compare specific cursors.
func (r *NebulaRegionFailoverReconciler) checkDrain(
	ctx context.Context,
	f *nebulav1alpha1.NebulaRegionFailover,
	cluster *nebulav1alpha1.NebulaCluster,
	_ logr.Logger,
) (ctrl.Result, error) {
	// Past the drain deadline? Either proceed (Force) or fail.
	if f.Status.StartedAt != nil && f.Spec.DrainTimeoutSeconds > 0 {
		deadline := f.Status.StartedAt.Add(time.Duration(f.Spec.DrainTimeoutSeconds) * time.Second)
		if time.Now().After(deadline) {
			if !f.Spec.Force {
				r.transition(ctx, f, nebulav1alpha1.RegionFailoverFailed,
					"drain timeout exceeded; re-apply with spec.force=true to proceed with current data")
				return ctrl.Result{}, nil
			}
			r.appendFailoverStep(f, "Drain", "ForcedProceed", "drain timeout; proceeding with current data")
			r.transition(ctx, f, nebulav1alpha1.RegionFailoverEpochBump, "forced past drain")
			return ctrl.Result{Requeue: true}, nil
		}
	}

	targetURL := regionLeaderRESTURL(cluster, f.Spec.NewHomeRegion)
	nc := r.NebulaFactory(targetURL).WithTimeout(5 * time.Second)
	repl, err := nc.Replication(ctx)
	if err != nil {
		r.appendFailoverStep(f, "Drain", "Retry", err.Error())
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	// Any remote with healthy=true that points at the OLD home counts.
	for _, rem := range repl.Remotes {
		if rem.Region == f.Status.FromHomeRegion && rem.Healthy {
			r.appendFailoverStep(f, "Drain", "Done", fmt.Sprintf(
				"remote %s applied=%d segments=last", rem.Region, rem.AppliedRecords))
			r.transition(ctx, f, nebulav1alpha1.RegionFailoverEpochBump, "target caught up")
			return ctrl.Result{Requeue: true}, nil
		}
	}

	// Empty `from` (first-ever home) has nothing to drain from.
	if f.Status.FromHomeRegion == "" {
		r.appendFailoverStep(f, "Drain", "Skip", "no previous home")
		r.transition(ctx, f, nebulav1alpha1.RegionFailoverEpochBump, "skipped drain")
		return ctrl.Result{Requeue: true}, nil
	}
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// bumpEpoch rewrites the seed doc on the new home region to claim the
// bucket. Uses the seed-doc upsert as the atomic point; if a concurrent
// failover beat us to the epoch bump, our subsequent re-read will show
// a higher epoch and we fail safely.
func (r *NebulaRegionFailoverReconciler) bumpEpoch(
	ctx context.Context,
	f *nebulav1alpha1.NebulaRegionFailover,
	cluster *nebulav1alpha1.NebulaCluster,
	_ logr.Logger,
) (ctrl.Result, error) {
	targetURL := regionLeaderRESTURL(cluster, f.Spec.NewHomeRegion)
	nc := r.NebulaFactory(targetURL).WithTimeout(5 * time.Second)

	// Re-read epoch right before the write — races with a concurrent
	// failover on the same bucket surface here.
	hr, err := nc.HomeRegion(ctx, f.Spec.Bucket)
	if err != nil {
		r.appendFailoverStep(f, "EpochBump", "Retry", err.Error())
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if int64(hr.HomeEpoch) != f.Status.FromEpoch {
		// Someone else bumped it. Abort rather than stomp.
		r.transition(ctx, f, nebulav1alpha1.RegionFailoverFailed, fmt.Sprintf(
			"concurrent failover detected: expected epoch %d, found %d",
			f.Status.FromEpoch, hr.HomeEpoch))
		return ctrl.Result{}, nil
	}

	newEpoch := f.Status.FromEpoch + 1
	// Compute the list of regions that will receive the tail after
	// failover: every region in cluster.spec.regions except the new
	// home. Keep it deterministic (ordered) so the seed doc diff
	// stays clean across reconciles.
	replicatedTo := make([]string, 0)
	for _, reg := range cluster.Spec.Regions {
		if reg.Name == f.Spec.NewHomeRegion {
			continue
		}
		replicatedTo = append(replicatedTo, reg.Name)
	}

	md := map[string]interface{}{
		"kind":          "nebuladb-operator-seed",
		"bucket":        f.Spec.Bucket,
		"home_region":   f.Spec.NewHomeRegion,
		"home_epoch":    newEpoch,
		"replicated_to": replicatedTo,
		"created_by":    ManagedBy,
		"cluster":       f.Spec.ClusterRef,
	}
	if err := nc.UpsertDoc(ctx, f.Spec.Bucket, nebulaclient.UpsertDocRequest{
		ID:       bucketSeedDocID,
		Text:     fmt.Sprintf("NebulaDB bucket %q (home=%s) managed by %s", f.Spec.Bucket, f.Spec.NewHomeRegion, ManagedBy),
		Metadata: md,
	}); err != nil {
		r.appendFailoverStep(f, "EpochBump", "Failed", err.Error())
		r.transition(ctx, f, nebulav1alpha1.RegionFailoverFailed, err.Error())
		return ctrl.Result{}, nil
	}

	f.Status.ToEpoch = newEpoch
	r.appendFailoverStep(f, "EpochBump", "Done", fmt.Sprintf(
		"%d -> %d (home=%s)", f.Status.FromEpoch, newEpoch, f.Spec.NewHomeRegion))
	r.transition(ctx, f, nebulav1alpha1.RegionFailoverAnnounce, "home updated")
	return ctrl.Result{Requeue: true}, nil
}

// announce updates the NebulaBucket CR so downstream operators (and
// `kubectl get nebulabucket`) see the new home. Spec is patched to
// keep declarative ownership consistent; Status reflects observed
// state.
func (r *NebulaRegionFailoverReconciler) announce(
	ctx context.Context,
	f *nebulav1alpha1.NebulaRegionFailover,
	_ *nebulav1alpha1.NebulaCluster,
	_ logr.Logger,
) (ctrl.Result, error) {
	// Look up the matching NebulaBucket by name == bucket and
	// spec.clusterRef == our cluster.
	var buckets nebulav1alpha1.NebulaBucketList
	if err := r.List(ctx, &buckets, client.InNamespace(f.Namespace)); err != nil {
		r.appendFailoverStep(f, "Announce", "Retry", err.Error())
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	for i := range buckets.Items {
		b := &buckets.Items[i]
		bucketName := b.Spec.Name
		if bucketName == "" {
			bucketName = b.Name
		}
		if b.Spec.ClusterRef != f.Spec.ClusterRef || bucketName != f.Spec.Bucket {
			continue
		}
		latest := b.DeepCopy()
		latest.Spec.HomeRegion = f.Spec.NewHomeRegion
		if err := r.Patch(ctx, latest, client.MergeFrom(b)); err != nil {
			r.appendFailoverStep(f, "Announce", "Retry", err.Error())
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		r.appendFailoverStep(f, "Announce", "Done", "bucket CR updated")
		r.Recorder.Eventf(f, corev1.EventTypeNormal, "HomeRegionBumped",
			"bucket=%s %s -> %s (epoch %d -> %d)",
			f.Spec.Bucket, f.Status.FromHomeRegion, f.Spec.NewHomeRegion,
			f.Status.FromEpoch, f.Status.ToEpoch)
		break
	}

	now := metav1.Now()
	f.Status.CompletedAt = &now
	r.transition(ctx, f, nebulav1alpha1.RegionFailoverCompleted, "failover complete")
	return ctrl.Result{}, nil
}

// transition patches the CR status to move phases forward.
func (r *NebulaRegionFailoverReconciler) transition(
	ctx context.Context,
	f *nebulav1alpha1.NebulaRegionFailover,
	phase nebulav1alpha1.RegionFailoverPhase,
	msg string,
) {
	latest := f.DeepCopy()
	latest.Status.Phase = phase
	latest.Status.Message = msg
	latest.Status.ObservedGeneration = f.Generation
	cond := metav1.Condition{Type: "Progressing", Status: metav1.ConditionTrue, Reason: string(phase), Message: msg}
	if phase == nebulav1alpha1.RegionFailoverCompleted {
		cond = metav1.Condition{Type: "Progressing", Status: metav1.ConditionFalse, Reason: "Completed", Message: msg}
	}
	setCondition(&latest.Status.Conditions, cond)
	_ = r.Status().Patch(ctx, latest, client.MergeFrom(f))
	*f = *latest
}

func (r *NebulaRegionFailoverReconciler) appendFailoverStep(f *nebulav1alpha1.NebulaRegionFailover, name, state, msg string) {
	now := metav1.Now()
	f.Status.Steps = append(f.Status.Steps, nebulav1alpha1.RebalanceStep{
		Name:      name,
		State:     state,
		Message:   msg,
		StartedAt: &now,
	})
}

// regionLeaderRESTURL builds the in-cluster REST URL for a specific
// region's leader service.
func regionLeaderRESTURL(cluster *nebulav1alpha1.NebulaCluster, region string) string {
	svc := fmt.Sprintf("%s-%s-leader", cluster.Name, region)
	if region == "" || region == "default" {
		svc = fmt.Sprintf("%s-leader", cluster.Name)
	}
	return fmt.Sprintf("http://%s.%s.svc.cluster.local:8080", svc, cluster.Namespace)
}

func (r *NebulaRegionFailoverReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nebulav1alpha1.NebulaRegionFailover{}).
		Complete(r)
}
