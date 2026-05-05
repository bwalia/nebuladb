package controllers

import (
	"context"
	"fmt"
	"sort"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	nebulav1alpha1 "github.com/bwalia/nebuladb-operator/api/v1alpha1"
)

const (
	finalizerBackupSchedule = "nebula.nebuladb.io/backupschedule-finalizer"
	defaultNebulactlImage   = "ghcr.io/bwalia/nebulactl:latest"
)

// NebulaBackupScheduleReconciler watches NebulaBackupSchedule CRs and
// owns the per-schedule CronJob lifecycle. The CronJob's pod runs
// `nebulactl backup start --wait`, which calls the cluster's
// `/admin/backup` endpoint and blocks until completion. Pod success/
// failure is reflected onto the schedule's status by this controller.
//
// Retention is applied each reconcile pass — sweep `NebulaBackup`
// CRs scoped to this schedule, compute the keep set, delete the rest
// (and best-effort their S3 objects via the same nebulactl image).
type NebulaBackupScheduleReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=nebula.nebuladb.io,resources=nebulabackupschedules,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nebula.nebuladb.io,resources=nebulabackupschedules/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nebula.nebuladb.io,resources=nebulabackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete

func (r *NebulaBackupScheduleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lg := log.FromContext(ctx).WithValues("schedule", req.NamespacedName)

	var sched nebulav1alpha1.NebulaBackupSchedule
	if err := r.Get(ctx, req.NamespacedName, &sched); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !sched.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&sched, finalizerBackupSchedule) {
			// CronJob is owner-referenced; GC handles deletion.
			controllerutil.RemoveFinalizer(&sched, finalizerBackupSchedule)
			return ctrl.Result{}, r.Update(ctx, &sched)
		}
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(&sched, finalizerBackupSchedule) {
		controllerutil.AddFinalizer(&sched, finalizerBackupSchedule)
		if err := r.Update(ctx, &sched); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Validate parent cluster exists; status emits a friendly
	// message if not, but we don't fail loudly — operators commonly
	// declare the schedule before the cluster.
	var cluster nebulav1alpha1.NebulaCluster
	if err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: sched.Spec.ClusterRef}, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			r.Recorder.Eventf(&sched, corev1.EventTypeWarning, "ClusterNotFound",
				"NebulaCluster %q not found in this namespace", sched.Spec.ClusterRef)
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	// Reconcile the CronJob.
	cron, err := r.buildCronJob(&sched, &cluster)
	if err != nil {
		r.Recorder.Eventf(&sched, corev1.EventTypeWarning, "ConfigError", "%v", err)
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}
	if err := r.upsertCronJob(ctx, &sched, cron); err != nil {
		return ctrl.Result{}, err
	}

	// Apply retention. Cheap to run every reconcile; bounded by the
	// number of backups under this schedule.
	if err := r.applyRetention(ctx, &sched); err != nil {
		lg.V(1).Info("retention sweep had errors; will retry", "err", err)
	}

	// Status: mirror the schedule into the CR. We don't try to
	// compute "next scheduled" (cron parsing in Go without a library
	// isn't fun); CronJob.status.lastScheduleTime is the canonical
	// answer when somebody really wants it.
	r.refreshStatus(ctx, &sched, cron.Name)
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

// buildCronJob constructs the per-schedule CronJob. The pod runs
// nebulactl with the schedule's storage flags; credentials come from
// the referenced Secret as env vars.
func (r *NebulaBackupScheduleReconciler) buildCronJob(
	sched *nebulav1alpha1.NebulaBackupSchedule,
	cluster *nebulav1alpha1.NebulaCluster,
) (*batchv1.CronJob, error) {
	if sched.Spec.Schedule == "" {
		return nil, fmt.Errorf("spec.schedule is required")
	}
	storageArgs, env, err := storageArgsAndEnv(&sched.Spec.Storage)
	if err != nil {
		return nil, err
	}
	image := sched.Spec.Image
	if image == "" {
		image = defaultNebulactlImage
	}
	resources := sched.Spec.Resources
	if resources.Requests == nil {
		resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("50m"),
			corev1.ResourceMemory: resource.MustParse("64Mi"),
		}
	}
	if resources.Limits == nil {
		resources.Limits = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		}
	}
	clusterURL := fmt.Sprintf("http://%s-leader.%s.svc.cluster.local:8080", cluster.Name, cluster.Namespace)

	args := []string{
		"backup", "start",
		"--cluster-name", cluster.Name,
		"--wait",
	}
	args = append(args, storageArgs...)

	suspend := sched.Spec.Suspend
	cron := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-backup", sched.Name),
			Namespace: sched.Namespace,
			Labels: map[string]string{
				LabelAppName:      "nebuladb-backup",
				LabelAppManagedBy: ManagedBy,
				LabelClusterName:  sched.Spec.ClusterRef,
				"nebula.nebuladb.io/schedule": sched.Name,
			},
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   sched.Spec.Schedule,
			Suspend:                    &suspend,
			SuccessfulJobsHistoryLimit: ptrInt32(3),
			FailedJobsHistoryLimit:     ptrInt32(3),
			ConcurrencyPolicy:          batchv1.ForbidConcurrent,
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					BackoffLimit: ptrInt32(1),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyOnFailure,
							Containers: []corev1.Container{{
								Name:    "nebulactl",
								Image:   image,
								Args:    args,
								Env:     append([]corev1.EnvVar{{Name: "NEBULA_URL", Value: clusterURL}}, env...),
								Resources: resources,
							}},
						},
					},
				},
			},
		},
	}
	if err := controllerutil.SetControllerReference(sched, cron, r.Scheme); err != nil {
		return nil, err
	}
	return cron, nil
}

func (r *NebulaBackupScheduleReconciler) upsertCronJob(ctx context.Context, owner *nebulav1alpha1.NebulaBackupSchedule, desired *batchv1.CronJob) error {
	var existing batchv1.CronJob
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), &existing)
	switch {
	case apierrors.IsNotFound(err):
		return r.Create(ctx, desired)
	case err != nil:
		return err
	}
	desired.ResourceVersion = existing.ResourceVersion
	return r.Update(ctx, desired)
}

// storageArgsAndEnv translates a BackupStorageSpec into the CLI flag
// list + env vars (for credentials). The two halves are returned
// separately so the caller can splice the env into a normal pod env
// list rather than re-encoding everything as flags.
func storageArgsAndEnv(s *nebulav1alpha1.BackupStorageSpec) ([]string, []corev1.EnvVar, error) {
	if s.Local != nil {
		return []string{"--local", s.Local.Path}, nil, nil
	}
	if s.S3 == nil {
		return nil, nil, fmt.Errorf("backup storage requires .s3 or .local")
	}
	args := []string{"--s3-bucket", s.S3.Bucket}
	if s.S3.Prefix != "" {
		args = append(args, "--s3-prefix", s.S3.Prefix)
	}
	if s.S3.Region != "" {
		args = append(args, "--s3-region", s.S3.Region)
	}
	if s.S3.Endpoint != "" {
		args = append(args, "--s3-endpoint", s.S3.Endpoint)
	}
	var env []corev1.EnvVar
	if s.S3.CredentialsSecretRef != nil {
		// Inject as standard AWS env names so the SDK picks them up.
		// We ignore the user-supplied keys list and require the
		// secret to use the canonical AWS_ACCESS_KEY_ID /
		// AWS_SECRET_ACCESS_KEY keys; documenting the convention is
		// simpler than threading two custom key names through.
		env = append(env,
			corev1.EnvVar{
				Name: "AWS_ACCESS_KEY_ID",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: s.S3.CredentialsSecretRef.Name},
						Key:                  "AWS_ACCESS_KEY_ID",
					},
				},
			},
			corev1.EnvVar{
				Name: "AWS_SECRET_ACCESS_KEY",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: s.S3.CredentialsSecretRef.Name},
						Key:                  "AWS_SECRET_ACCESS_KEY",
					},
				},
			},
		)
	}
	return args, env, nil
}

// applyRetention sweeps NebulaBackup CRs scoped to this schedule and
// deletes those that fall outside the retention window. Object-storage
// cleanup is intentionally NOT done here — the operator process
// shouldn't carry S3 credentials. The CronJob's pod can run a
// retention pass via `nebulactl` in a follow-up; for now we delete
// the CR and trust the storage's own lifecycle rules.
func (r *NebulaBackupScheduleReconciler) applyRetention(ctx context.Context, sched *nebulav1alpha1.NebulaBackupSchedule) error {
	pol := sched.Spec.Retention
	// All zeros = "keep everything" — fast path.
	if pol.KeepLast == 0 && pol.KeepDailyDays == 0 && pol.KeepWeeklyWeeks == 0 && pol.KeepMonthlyMonths == 0 {
		return nil
	}
	var list nebulav1alpha1.NebulaBackupList
	if err := r.List(ctx, &list, client.InNamespace(sched.Namespace)); err != nil {
		return err
	}
	// Only consider backups owned by this schedule + that succeeded;
	// a failed backup in the keep set would just be confusing.
	var attempts []nebulav1alpha1.NebulaBackup
	for i := range list.Items {
		b := &list.Items[i]
		if b.Spec.ScheduleRef == sched.Name && b.Status.Phase == "Completed" {
			attempts = append(attempts, *b)
		}
	}
	// Sort newest-first. Sort by completed_at when available, fall
	// back to creation time for partially-stamped CRs.
	sort.Slice(attempts, func(i, j int) bool {
		ti := backupAge(&attempts[i])
		tj := backupAge(&attempts[j])
		return ti.After(tj)
	})

	keep := map[string]struct{}{}
	if pol.KeepLast > 0 {
		for i := 0; i < int(pol.KeepLast) && i < len(attempts); i++ {
			keep[attempts[i].Name] = struct{}{}
		}
	}
	keepBucketed(attempts, pol.KeepDailyDays, 24*time.Hour, keep)
	keepBucketed(attempts, pol.KeepWeeklyWeeks, 7*24*time.Hour, keep)
	keepBucketed(attempts, pol.KeepMonthlyMonths, 30*24*time.Hour, keep)

	for i := range attempts {
		b := &attempts[i]
		if _, ok := keep[b.Name]; ok {
			continue
		}
		if err := r.Delete(ctx, b); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// keepBucketed picks one entry per time window for `count` windows.
// The newest entry within each window is kept.
func keepBucketed(items []nebulav1alpha1.NebulaBackup, count int32, window time.Duration, keep map[string]struct{}) {
	if count <= 0 || len(items) == 0 {
		return
	}
	now := time.Now()
	for w := 0; w < int(count); w++ {
		windowStart := now.Add(-time.Duration(w+1) * window)
		windowEnd := now.Add(-time.Duration(w) * window)
		// items is newest-first; pick the first one whose age falls
		// inside this window.
		for i := range items {
			t := backupAge(&items[i])
			if t.After(windowStart) && !t.After(windowEnd) {
				keep[items[i].Name] = struct{}{}
				break
			}
		}
	}
}

func backupAge(b *nebulav1alpha1.NebulaBackup) time.Time {
	if b.Status.CompletedAt != nil {
		return b.Status.CompletedAt.Time
	}
	return b.CreationTimestamp.Time
}

// refreshStatus updates the schedule's status with the most recent
// successful backup it has seen. Cheap — re-list every reconcile.
func (r *NebulaBackupScheduleReconciler) refreshStatus(ctx context.Context, sched *nebulav1alpha1.NebulaBackupSchedule, cronName string) {
	latest := sched.DeepCopy()
	latest.Status.ObservedGeneration = sched.Generation
	latest.Status.CronJobName = cronName

	var list nebulav1alpha1.NebulaBackupList
	if err := r.List(ctx, &list, client.InNamespace(sched.Namespace)); err == nil {
		var newest *nebulav1alpha1.NebulaBackup
		for i := range list.Items {
			b := &list.Items[i]
			if b.Spec.ScheduleRef != sched.Name || b.Status.Phase != "Completed" {
				continue
			}
			if newest == nil || backupAge(b).After(backupAge(newest)) {
				newest = b
			}
		}
		if newest != nil {
			latest.Status.LastBackupID = newest.Status.BackupID
			latest.Status.LastBackupAt = newest.Status.CompletedAt
		}
	}
	_ = r.Status().Patch(ctx, latest, client.MergeFrom(sched))
}

func (r *NebulaBackupScheduleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nebulav1alpha1.NebulaBackupSchedule{}).
		Owns(&batchv1.CronJob{}).
		Complete(r)
}

