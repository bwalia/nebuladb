package controllers

import (
	"context"
	"fmt"
	"time"

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

// NebulaBucketReconciler keeps a bucket's live state aligned with the CR.
//
// Since NebulaDB buckets are implicit (created on first doc insert), we
// "create" a bucket by upserting a well-known seed document with the
// operator's own ID prefix. This guarantees the bucket shows up in
// /admin/buckets and lets users discover it via the API.
//
// On delete (if DeletePolicy=Empty) we call POST /admin/bucket/:b/empty.
// Retain leaves the bucket in place.
type NebulaBucketReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Recorder      record.EventRecorder
	NebulaFactory func(base string) *nebulaclient.Client
}

const bucketSeedDocID = "__nebuladb_operator_seed__"

// +kubebuilder:rbac:groups=nebula.nebuladb.io,resources=nebulabuckets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nebula.nebuladb.io,resources=nebulabuckets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nebula.nebuladb.io,resources=nebulabuckets/finalizers,verbs=update

func (r *NebulaBucketReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lg := log.FromContext(ctx).WithValues("nebulabucket", req.NamespacedName)

	var bucket nebulav1alpha1.NebulaBucket
	if err := r.Get(ctx, req.NamespacedName, &bucket); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Resolve the parent cluster up-front; we need its service DNS for every
	// admin call below.
	var cluster nebulav1alpha1.NebulaCluster
	if err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: bucket.Spec.ClusterRef}, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			r.Recorder.Eventf(&bucket, "Warning", "ClusterNotFound", "cluster %q not found", bucket.Spec.ClusterRef)
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	leaderURL := fmt.Sprintf("http://%s-leader.%s.svc.cluster.local:8080", cluster.Name, cluster.Namespace)
	nc := r.NebulaFactory(leaderURL).WithTimeout(5 * time.Second)

	if !bucket.DeletionTimestamp.IsZero() {
		return r.handleDelete(ctx, &bucket, nc)
	}

	if !controllerutil.ContainsFinalizer(&bucket, FinalizerBucket) {
		controllerutil.AddFinalizer(&bucket, FinalizerBucket)
		if err := r.Update(ctx, &bucket); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	name := bucket.Spec.Name
	if name == "" {
		name = bucket.Name
	}

	// Seed the bucket by upserting the operator marker document. Idempotent.
	md := map[string]interface{}{
		"kind":       "nebuladb-operator-seed",
		"bucket":     name,
		"type":       string(bucket.Spec.Type),
		"created_by": ManagedBy,
		"cluster":    bucket.Spec.ClusterRef,
	}
	if bucket.Spec.Indexing.ChunkChars > 0 {
		md["chunk_chars"] = bucket.Spec.Indexing.ChunkChars
	}
	if bucket.Spec.Indexing.ChunkOverlap > 0 {
		md["chunk_overlap"] = bucket.Spec.Indexing.ChunkOverlap
	}
	if bucket.Spec.Indexing.Metric != "" {
		md["metric"] = bucket.Spec.Indexing.Metric
	}
	for k, v := range bucket.Spec.Labels {
		md["label_"+k] = v
	}
	err := nc.UpsertDoc(ctx, name, nebulaclient.UpsertDocRequest{
		ID:       bucketSeedDocID,
		Text:     fmt.Sprintf("NebulaDB bucket %q managed by %s", name, ManagedBy),
		Metadata: md,
	})
	if err != nil {
		if nebulaclient.IsReadOnlyFollower(err) {
			// Shouldn't happen (we target the leader service) but handle it
			// defensively — indicates a routing / service-selector bug.
			r.Recorder.Eventf(&bucket, "Warning", "WriteToFollower", "leader service routed to follower: %v", err)
		}
		lg.Error(err, "seeding bucket failed")
		r.patchBucketStatus(ctx, &bucket, "Failed", 0, 0, nil, err.Error())
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Refresh stats.
	docCount, parents, top, err := r.fetchBucketStats(ctx, nc, name)
	if err != nil {
		lg.V(1).Info("bucket stats probe failed", "err", err)
	}
	r.patchBucketStatus(ctx, &bucket, "Ready", docCount, parents, top, "")
	return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
}

func (r *NebulaBucketReconciler) handleDelete(ctx context.Context, bucket *nebulav1alpha1.NebulaBucket, nc *nebulaclient.Client) (ctrl.Result, error) {
	if controllerutil.ContainsFinalizer(bucket, FinalizerBucket) {
		if bucket.Spec.DeletePolicy == "Empty" {
			name := bucket.Spec.Name
			if name == "" {
				name = bucket.Name
			}
			if err := nc.EmptyBucket(ctx, name); err != nil {
				r.Recorder.Eventf(bucket, "Warning", "EmptyFailed", "%v", err)
				return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
			}
		}
		controllerutil.RemoveFinalizer(bucket, FinalizerBucket)
		if err := r.Update(ctx, bucket); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

func (r *NebulaBucketReconciler) fetchBucketStats(ctx context.Context, nc *nebulaclient.Client, name string) (int64, int64, []string, error) {
	resp, err := nc.Buckets(ctx, 10)
	if err != nil {
		return 0, 0, nil, err
	}
	for _, b := range resp.Buckets {
		if b.Name == name {
			return b.Docs, b.ParentDocs, b.TopKeys, nil
		}
	}
	return 0, 0, nil, nil
}

func (r *NebulaBucketReconciler) patchBucketStatus(ctx context.Context, bucket *nebulav1alpha1.NebulaBucket, phase string, docs, parents int64, top []string, msg string) {
	latest := bucket.DeepCopy()
	latest.Status.Phase = phase
	latest.Status.DocCount = docs
	latest.Status.ParentDocs = parents
	latest.Status.TopKeys = top
	latest.Status.ObservedGeneration = bucket.Generation
	now := metav1.Now()
	latest.Status.LastSeeded = &now

	cond := metav1.Condition{
		Type:   "Ready",
		Status: metav1.ConditionTrue,
		Reason: phase,
	}
	if phase != "Ready" {
		cond.Status = metav1.ConditionFalse
		cond.Message = msg
	}
	setCondition(&latest.Status.Conditions, cond)
	_ = r.Status().Patch(ctx, latest, client.MergeFrom(bucket))
}

func (r *NebulaBucketReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nebulav1alpha1.NebulaBucket{}).
		Complete(r)
}
