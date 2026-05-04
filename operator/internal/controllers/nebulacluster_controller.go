package controllers

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	nebulav1alpha1 "github.com/bwalia/nebuladb-operator/api/v1alpha1"
	"github.com/bwalia/nebuladb-operator/internal/nebulaclient"
)

// NebulaClusterReconciler owns the full cluster lifecycle. The flow:
//
//   1. Ensure finalizer present.
//   2. For each region: reconcile leader STS, follower STS, services.
//   3. Probe each pod's /healthz and populate NodeStatus list.
//   4. Handle version drift — snapshot leader, then delete leader pod to let
//      STS recreate with new image (OnDelete update strategy).
//   5. Update status.phase + conditions.
//
// Rationale for OnDelete + explicit pod delete: NebulaDB is single-writer
// per node. A rolling STS update would bring a new pod up while the old one
// still serves writes — both backed by their own in-memory state — which is
// exactly what we must prevent. Snapshot-then-delete gives us serialization.
type NebulaClusterReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Recorder      record.EventRecorder
	NebulaFactory func(base string) *nebulaclient.Client
}

// +kubebuilder:rbac:groups=nebula.nebuladb.io,resources=nebulaclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nebula.nebuladb.io,resources=nebulaclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nebula.nebuladb.io,resources=nebulaclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete

func (r *NebulaClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lg := log.FromContext(ctx).WithValues("nebulacluster", req.NamespacedName)

	var cluster nebulav1alpha1.NebulaCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !cluster.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, &cluster, lg)
	}

	if !controllerutil.ContainsFinalizer(&cluster, FinalizerCluster) {
		controllerutil.AddFinalizer(&cluster, FinalizerCluster)
		if err := r.Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Fast-path status fields needed for downstream controllers (bucket,
	// rebalance) to see desired shape.
	if err := r.reconcileRegions(ctx, &cluster, lg); err != nil {
		r.setFailed(ctx, &cluster, "ReconcileFailed", err.Error())
		return ctrl.Result{RequeueAfter: 15 * time.Second}, err
	}

	if err := r.reconcileUpgrade(ctx, &cluster, lg); err != nil {
		r.setFailed(ctx, &cluster, "UpgradeFailed", err.Error())
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	if err := r.updateStatus(ctx, &cluster, lg); err != nil {
		lg.Error(err, "updateStatus")
	}

	// Keep a modest requeue so we re-probe health even when nothing's changed.
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// reconcileRegions owns the "desired shape" portion of the loop. It does not
// touch version — the upgrade controller handles that.
func (r *NebulaClusterReconciler) reconcileRegions(ctx context.Context, cluster *nebulav1alpha1.NebulaCluster, lg logr.Logger) error {
	for _, region := range regionsOrDefault(cluster) {
		if err := r.reconcileOneRegion(ctx, cluster, region, lg); err != nil {
			return fmt.Errorf("region %q: %w", region.Name, err)
		}
	}
	return nil
}

func (r *NebulaClusterReconciler) reconcileOneRegion(ctx context.Context, cluster *nebulav1alpha1.NebulaCluster, region nebulav1alpha1.RegionSpec, lg logr.Logger) error {
	// 1. Headless + client services (leader).
	if err := r.upsertService(ctx, cluster, buildHeadlessService(cluster, region, ComponentLeader)); err != nil {
		return err
	}
	if err := r.upsertService(ctx, cluster, buildClientService(cluster, region, ComponentLeader)); err != nil {
		return err
	}

	// 2. Leader StatefulSet.
	leaderSTS := buildStatefulSet(cluster, region, ComponentLeader)
	if err := r.upsertStatefulSet(ctx, cluster, leaderSTS); err != nil {
		return err
	}

	// 3. Follower services + STS — even when disabled, we reconcile to zero
	// replicas so users flipping the toggle get an immediate scale-down.
	if err := r.upsertService(ctx, cluster, buildHeadlessService(cluster, region, ComponentFollower)); err != nil {
		return err
	}
	if err := r.upsertService(ctx, cluster, buildClientService(cluster, region, ComponentFollower)); err != nil {
		return err
	}
	followerSTS := buildStatefulSet(cluster, region, ComponentFollower)
	if err := r.upsertStatefulSet(ctx, cluster, followerSTS); err != nil {
		return err
	}

	return nil
}

func (r *NebulaClusterReconciler) upsertService(ctx context.Context, owner *nebulav1alpha1.NebulaCluster, desired *corev1.Service) error {
	if err := controllerutil.SetControllerReference(owner, desired, r.Scheme); err != nil {
		return err
	}
	var existing corev1.Service
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), &existing)
	switch {
	case apierrors.IsNotFound(err):
		return r.Create(ctx, desired)
	case err != nil:
		return err
	}
	// Preserve ClusterIP (immutable), merge selector + ports.
	desired.Spec.ClusterIP = existing.Spec.ClusterIP
	desired.Spec.ClusterIPs = existing.Spec.ClusterIPs
	desired.ResourceVersion = existing.ResourceVersion
	return r.Update(ctx, desired)
}

func (r *NebulaClusterReconciler) upsertStatefulSet(ctx context.Context, owner *nebulav1alpha1.NebulaCluster, desired *appsv1.StatefulSet) error {
	if err := controllerutil.SetControllerReference(owner, desired, r.Scheme); err != nil {
		return err
	}
	var existing appsv1.StatefulSet
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), &existing)
	switch {
	case apierrors.IsNotFound(err):
		return r.Create(ctx, desired)
	case err != nil:
		return err
	}
	// Preserve selector (immutable once created).
	desired.Spec.Selector = existing.Spec.Selector
	desired.Spec.ServiceName = existing.Spec.ServiceName
	desired.ResourceVersion = existing.ResourceVersion
	return r.Update(ctx, desired)
}

// reconcileUpgrade handles version bumps. For each region leader pod, if the
// running pod's image tag differs from spec.version, we:
//
//   1. Snapshot via POST /admin/snapshot (so followers have a clean recovery
//      point if they were mid-replay).
//   2. Update the STS template (already done via upsertStatefulSet).
//   3. Delete the leader pod — STS OnDelete strategy recreates it with the
//      new image.
//
// Followers are handled via the same pattern but without the snapshot step.
// The SwapRebalance strategy is accepted but logs "not implemented" and
// falls back to Recreate. Swap requires a rebalance engine in NebulaDB that
// does not exist yet (see NebulaRebalance controller for the full stub).
func (r *NebulaClusterReconciler) reconcileUpgrade(ctx context.Context, cluster *nebulav1alpha1.NebulaCluster, lg logr.Logger) error {
	if cluster.Spec.UpgradeStrategy == nebulav1alpha1.UpgradeSwapRebalance {
		r.Recorder.Event(cluster, corev1.EventTypeWarning, "SwapRebalanceUnsupported",
			"SwapRebalance strategy is not implemented in NebulaDB yet; falling back to Recreate")
	}

	for _, region := range regionsOrDefault(cluster) {
		if err := r.upgradeRegion(ctx, cluster, region, lg); err != nil {
			return fmt.Errorf("upgrade region %q: %w", region.Name, err)
		}
	}
	return nil
}

func (r *NebulaClusterReconciler) upgradeRegion(ctx context.Context, cluster *nebulav1alpha1.NebulaCluster, region nebulav1alpha1.RegionSpec, lg logr.Logger) error {
	// Only act once the STS spec carries the new version — upsertStatefulSet
	// already did that before this point.
	leaderPod := fmt.Sprintf("%s-0", leaderSTSName(cluster, region.Name))
	needs, err := r.podNeedsRefresh(ctx, cluster.Namespace, leaderPod, cluster.Spec.Version)
	if err != nil {
		return err
	}
	if needs {
		lg.Info("upgrading leader", "region", region.Name, "pod", leaderPod, "version", cluster.Spec.Version)
		r.Recorder.Eventf(cluster, corev1.EventTypeNormal, "LeaderUpgradeStart", "Upgrading leader %s to %s", leaderPod, cluster.Spec.Version)

		svcURL := fmt.Sprintf("http://%s.%s.svc.cluster.local:8080", leaderServiceName(cluster, region.Name), cluster.Namespace)
		nc := r.NebulaFactory(svcURL)
		if snap, err := nc.Snapshot(ctx); err != nil {
			lg.Error(err, "snapshot before upgrade failed; continuing")
			r.Recorder.Eventf(cluster, corev1.EventTypeWarning, "PreUpgradeSnapshotFailed", "%v", err)
		} else {
			now := metav1.Now()
			cluster.Status.LastSnapshotAt = &now
			cluster.Status.LastSnapshotPath = snap.Path
		}
		if err := r.deletePod(ctx, cluster.Namespace, leaderPod); err != nil {
			return err
		}
		r.Recorder.Eventf(cluster, corev1.EventTypeNormal, "LeaderUpgradeTriggered", "Deleted pod %s for recreate", leaderPod)
	}

	// Followers: delete any out-of-date follower pod one at a time.
	if cluster.Spec.Replication.Enabled && cluster.Spec.Replication.Followers > 0 {
		for i := int32(0); i < cluster.Spec.Replication.Followers; i++ {
			pod := fmt.Sprintf("%s-%d", followerSTSName(cluster, region.Name), i)
			needs, err := r.podNeedsRefresh(ctx, cluster.Namespace, pod, cluster.Spec.Version)
			if err != nil {
				return err
			}
			if needs {
				lg.Info("upgrading follower", "region", region.Name, "pod", pod)
				if err := r.deletePod(ctx, cluster.Namespace, pod); err != nil {
					return err
				}
				// Only take down one follower per pass to preserve read availability.
				return nil
			}
		}
	}
	return nil
}

func (r *NebulaClusterReconciler) podNeedsRefresh(ctx context.Context, ns, name, wantVersion string) (bool, error) {
	var pod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if pod.Labels[LabelAppVersion] == wantVersion {
		return false, nil
	}
	for _, c := range pod.Spec.Containers {
		if c.Name == "nebula" {
			// Tag must match what we expect; fallback is the label check above.
			return !endsWithTag(c.Image, wantVersion), nil
		}
	}
	return true, nil
}

func endsWithTag(image, tag string) bool {
	if image == "" || tag == "" {
		return false
	}
	return len(image) >= len(tag)+1 && image[len(image)-len(tag)-1] == ':' && image[len(image)-len(tag):] == tag
}

func (r *NebulaClusterReconciler) deletePod(ctx context.Context, ns, name string) error {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// updateStatus probes /healthz across leader + follower pods and writes a
// consolidated view onto the CR.
func (r *NebulaClusterReconciler) updateStatus(ctx context.Context, cluster *nebulav1alpha1.NebulaCluster, lg logr.Logger) error {
	latest := cluster.DeepCopy()
	latest.Status.ObservedGeneration = cluster.Generation
	latest.Status.CurrentVersion = cluster.Spec.Version
	latest.Status.DesiredReplicas = 1 + cluster.Spec.Replication.Followers*int32(len(regionsOrDefault(cluster)))

	var ready int32
	var leader *nebulav1alpha1.NodeStatus
	var followers []nebulav1alpha1.NodeStatus

	for _, region := range regionsOrDefault(cluster) {
		leaderPod := fmt.Sprintf("%s-0", leaderSTSName(cluster, region.Name))
		if ns := r.probePod(ctx, cluster, region.Name, leaderPod, ComponentLeader); ns != nil {
			leader = ns
			if ns.Healthy {
				ready++
			}
		}
		if cluster.Spec.Replication.Enabled {
			for i := int32(0); i < cluster.Spec.Replication.Followers; i++ {
				pod := fmt.Sprintf("%s-%d", followerSTSName(cluster, region.Name), i)
				if ns := r.probePod(ctx, cluster, region.Name, pod, ComponentFollower); ns != nil {
					followers = append(followers, *ns)
					if ns.Healthy {
						ready++
					}
				}
			}
		}
	}

	latest.Status.Leader = leader
	latest.Status.Followers = followers
	latest.Status.ReadyReplicas = ready

	switch {
	case ready == 0:
		latest.Status.Phase = nebulav1alpha1.PhaseProvisioning
	case ready < latest.Status.DesiredReplicas:
		latest.Status.Phase = nebulav1alpha1.PhaseDegraded
	default:
		latest.Status.Phase = nebulav1alpha1.PhaseRunning
	}

	setCondition(&latest.Status.Conditions, metav1.Condition{
		Type:   "Ready",
		Status: boolToCondStatus(ready > 0 && ready == latest.Status.DesiredReplicas),
		Reason: string(latest.Status.Phase),
	})

	return r.Status().Patch(ctx, latest, client.MergeFrom(cluster))
}

// probePod constructs a client addressing the pod's headless DNS entry.
// Auth is intentionally omitted — /healthz is outside the auth middleware.
func (r *NebulaClusterReconciler) probePod(ctx context.Context, cluster *nebulav1alpha1.NebulaCluster, region, pod, role string) *nebulav1alpha1.NodeStatus {
	svc := headlessServiceName(cluster, region, role)
	url := fmt.Sprintf("http://%s.%s.%s.svc.cluster.local:8080", pod, svc, cluster.Namespace)
	nc := r.NebulaFactory(url).WithTimeout(3 * time.Second)
	now := time.Now().UTC().Format(time.RFC3339)
	health, err := nc.Healthz(ctx)
	ns := &nebulav1alpha1.NodeStatus{
		Name:          pod,
		Role:          role,
		Region:        region,
		Address:       url,
		LastProbeTime: now,
	}
	if err != nil {
		ns.Healthy = false
		return ns
	}
	ns.Healthy = health.Status == "ok"
	ns.Docs = health.Docs

	if role == ComponentFollower {
		// Follower lag probing is best-effort. A busy cluster can reject us
		// with rate limits; tolerate any error.
		if rep, err := nc.Replication(ctx); err == nil {
			ns.LagBytes = rep.LagBytes
		}
	}
	return ns
}

func (r *NebulaClusterReconciler) handleDeletion(ctx context.Context, cluster *nebulav1alpha1.NebulaCluster, lg logr.Logger) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(cluster, FinalizerCluster) {
		return ctrl.Result{}, nil
	}
	// Best-effort: snapshot the leader so the PVC has a consistent capture,
	// then let garbage collection remove child resources.
	for _, region := range regionsOrDefault(cluster) {
		url := fmt.Sprintf("http://%s.%s.svc.cluster.local:8080", leaderServiceName(cluster, region.Name), cluster.Namespace)
		nc := r.NebulaFactory(url).WithTimeout(3 * time.Second)
		if _, err := nc.Snapshot(ctx); err != nil {
			lg.V(1).Info("final snapshot failed; proceeding with delete", "err", err)
		}
	}
	controllerutil.RemoveFinalizer(cluster, FinalizerCluster)
	return ctrl.Result{}, r.Update(ctx, cluster)
}

func (r *NebulaClusterReconciler) setFailed(ctx context.Context, cluster *nebulav1alpha1.NebulaCluster, reason, msg string) {
	latest := cluster.DeepCopy()
	latest.Status.Phase = nebulav1alpha1.PhaseFailed
	setCondition(&latest.Status.Conditions, metav1.Condition{
		Type:    "Ready",
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: msg,
	})
	_ = r.Status().Patch(ctx, latest, client.MergeFrom(cluster))
}

func setCondition(existing *[]metav1.Condition, c metav1.Condition) {
	if c.LastTransitionTime.IsZero() {
		c.LastTransitionTime = metav1.Now()
	}
	for i, cur := range *existing {
		if cur.Type == c.Type {
			if cur.Status == c.Status && cur.Reason == c.Reason {
				return // no-op
			}
			(*existing)[i] = c
			return
		}
	}
	*existing = append(*existing, c)
}

func boolToCondStatus(b bool) metav1.ConditionStatus {
	if b {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

func (r *NebulaClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nebulav1alpha1.NebulaCluster{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
