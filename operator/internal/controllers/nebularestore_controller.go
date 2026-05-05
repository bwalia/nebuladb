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

var _ logr.Logger

const finalizerRestore = "nebula.nebuladb.io/restore-finalizer"

// NebulaRestoreReconciler drives a NebulaRestore CR by calling the
// target cluster's POST /admin/restore endpoint, then polling for
// completion. The actual heavy lifting (download, sha256-verify,
// unpack) happens server-side; this controller is a state-machine
// thin layer around the REST surface.
type NebulaRestoreReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Recorder      record.EventRecorder
	NebulaFactory func(base string) *nebulaclient.Client
}

// +kubebuilder:rbac:groups=nebula.nebuladb.io,resources=nebularestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nebula.nebuladb.io,resources=nebularestores/status,verbs=get;update;patch

func (r *NebulaRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lg := log.FromContext(ctx).WithValues("restore", req.NamespacedName)

	var rs nebulav1alpha1.NebulaRestore
	if err := r.Get(ctx, req.NamespacedName, &rs); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Terminal states — nothing more to do.
	if rs.Status.Phase == "Completed" || rs.Status.Phase == "Failed" {
		return ctrl.Result{}, nil
	}

	if !rs.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&rs, finalizerRestore) {
			controllerutil.RemoveFinalizer(&rs, finalizerRestore)
			return ctrl.Result{}, r.Update(ctx, &rs)
		}
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(&rs, finalizerRestore) {
		controllerutil.AddFinalizer(&rs, finalizerRestore)
		if err := r.Update(ctx, &rs); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Resolve the target cluster.
	var cluster nebulav1alpha1.NebulaCluster
	if err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: rs.Spec.TargetClusterRef}, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			r.transitionRestore(ctx, &rs, "Failed",
				fmt.Sprintf("target cluster %q not found", rs.Spec.TargetClusterRef))
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	leaderURL := fmt.Sprintf("http://%s-leader.%s.svc.cluster.local:8080", cluster.Name, cluster.Namespace)
	nc := r.NebulaFactory(leaderURL).WithTimeout(30 * time.Second)

	switch rs.Status.Phase {
	case "":
		return r.startRestore(ctx, &rs, nc, lg)
	case "Pending", "Downloading", "Verifying", "Unpacking", "Replaying":
		return r.pollRestore(ctx, &rs, nc)
	}
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

// startRestore POSTs to /admin/restore and records the returned id.
func (r *NebulaRestoreReconciler) startRestore(
	ctx context.Context,
	rs *nebulav1alpha1.NebulaRestore,
	nc *nebulaclient.Client,
	_ logr.Logger,
) (ctrl.Result, error) {
	now := metav1.Now()
	rs.Status.StartedAt = &now

	storageJSON, err := nebulaclient.StorageSpecJSON(rs.Spec.Storage.S3Json(), rs.Spec.Storage.LocalJson())
	if err != nil {
		r.transitionRestore(ctx, rs, "Failed", err.Error())
		return ctrl.Result{}, nil
	}
	body := map[string]interface{}{
		"backup_id": rs.Spec.SourceBackupID,
		"data_dir":  "/var/lib/nebuladb",
		"storage":   storageJSON,
	}
	resp, err := nc.StartRestore(ctx, body)
	if err != nil {
		r.Recorder.Eventf(rs, corev1.EventTypeWarning, "RestoreStartFailed", "%v", err)
		// Don't fail terminally — transient errors should retry.
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	rs.Status.RestoreID = resp.RestoreID
	r.transitionRestore(ctx, rs, "Pending", "restore submitted")
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// pollRestore reads /admin/restore/:id and reflects phase + bytes.
func (r *NebulaRestoreReconciler) pollRestore(
	ctx context.Context,
	rs *nebulav1alpha1.NebulaRestore,
	nc *nebulaclient.Client,
) (ctrl.Result, error) {
	if rs.Status.RestoreID == "" {
		// Lost track somehow — re-submit.
		rs.Status.Phase = ""
		return ctrl.Result{Requeue: true}, nil
	}
	st, err := nc.RestoreStatus(ctx, rs.Status.RestoreID)
	if err != nil {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	switch st.Phase {
	case "completed":
		now := metav1.Now()
		rs.Status.CompletedAt = &now
		r.transitionRestore(ctx, rs, "Completed", "restore complete; reboot the target cluster pointed at the restored data dir")
		return ctrl.Result{}, nil
	case "failed":
		r.transitionRestore(ctx, rs, "Failed", st.Error)
		return ctrl.Result{}, nil
	default:
		// Still running.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
}

func (r *NebulaRestoreReconciler) transitionRestore(ctx context.Context, rs *nebulav1alpha1.NebulaRestore, phase, msg string) {
	latest := rs.DeepCopy()
	latest.Status.Phase = phase
	latest.Status.Message = msg
	latest.Status.ObservedGeneration = rs.Generation
	cond := metav1.Condition{
		Type:    "Progressing",
		Status:  metav1.ConditionTrue,
		Reason:  phase,
		Message: msg,
	}
	if phase == "Completed" || phase == "Failed" {
		cond.Status = metav1.ConditionFalse
	}
	setCondition(&latest.Status.Conditions, cond)
	_ = r.Status().Patch(ctx, latest, client.MergeFrom(rs))
	*rs = *latest
}

func (r *NebulaRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nebulav1alpha1.NebulaRestore{}).
		Complete(r)
}
