package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// S3StorageSpec mirrors the server's StorageSpec::S3 wire shape.
// Credentials live in a Secret so the CR never carries them.
type S3StorageSpec struct {
	// +kubebuilder:validation:Required
	Bucket   string `json:"bucket"`
	Prefix   string `json:"prefix,omitempty"`
	Region   string `json:"region,omitempty"`
	// Endpoint override for non-AWS targets (MinIO, R2, etc).
	Endpoint string `json:"endpoint,omitempty"`
	// CredentialsSecretRef points at a Secret with two keys —
	// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. The operator
	// injects them into the CronJob's pod env so neither the CR
	// nor the manager process sees the values.
	CredentialsSecretRef *SecretKeySelector `json:"credentialsSecretRef,omitempty"`
}

// LocalStorageSpec is for tests + mounted-volume backups.
type LocalStorageSpec struct {
	// +kubebuilder:validation:Required
	Path string `json:"path"`
}

// BackupStorageSpec is a discriminated union: exactly one of S3 or
// Local must be set. The cluster controller's webhook (future) can
// enforce; for now we validate at controller time.
type BackupStorageSpec struct {
	S3    *S3StorageSpec    `json:"s3,omitempty"`
	Local *LocalStorageSpec `json:"local,omitempty"`
}

// S3Json renders the S3 spec as a JSON-friendly map for the
// nebulaclient. Returns nil when not configured.
func (s *BackupStorageSpec) S3Json() map[string]interface{} {
	if s.S3 == nil {
		return nil
	}
	out := map[string]interface{}{
		"bucket": s.S3.Bucket,
		"prefix": s.S3.Prefix,
		"region": s.S3.Region,
	}
	if s.S3.Endpoint != "" {
		out["endpoint"] = s.S3.Endpoint
	}
	return out
}

// LocalJson renders the local spec as a JSON-friendly map. Returns
// nil when not configured.
func (s *BackupStorageSpec) LocalJson() map[string]interface{} {
	if s.Local == nil {
		return nil
	}
	return map[string]interface{}{"path": s.Local.Path}
}

// RetentionPolicy decides which old backups survive a retention sweep.
// Each field is optional; an entry counts toward retention if any of
// the rules match it (union semantics — common in backup tools).
type RetentionPolicy struct {
	// KeepLast keeps the N most recent regardless of age.
	KeepLast int32 `json:"keepLast,omitempty"`
	// KeepDailyDays — keep one per day for N days.
	KeepDailyDays int32 `json:"keepDailyDays,omitempty"`
	// KeepWeeklyWeeks — keep one per week for N weeks.
	KeepWeeklyWeeks int32 `json:"keepWeeklyWeeks,omitempty"`
	// KeepMonthlyMonths — keep one per month for N months.
	KeepMonthlyMonths int32 `json:"keepMonthlyMonths,omitempty"`
}

// NebulaBackupScheduleSpec is the desired backup schedule.
type NebulaBackupScheduleSpec struct {
	// +kubebuilder:validation:Required
	ClusterRef string `json:"clusterRef"`
	// Cron expression in the cluster's local timezone.
	// +kubebuilder:validation:Required
	Schedule  string             `json:"schedule"`
	Storage   BackupStorageSpec  `json:"storage"`
	Retention RetentionPolicy    `json:"retention,omitempty"`
	// Throttle reserved for future use; today it's documentation.
	Throttle *BackupThrottle `json:"throttle,omitempty"`
	// Suspend pauses the schedule without deleting the CR. Mirrors
	// the same field on Kubernetes CronJob.
	Suspend bool `json:"suspend,omitempty"`
	// Image is the nebulactl image used by the generated CronJob.
	// Defaults to ghcr.io/bwalia/nebulactl:latest if unset.
	Image string `json:"image,omitempty"`
	// Resources for the CronJob's pod.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// BackupThrottle is reserved for v1.1 — the server-side engine
// doesn't honor a bandwidth cap yet. Kept in the spec so users can
// declare their intent without a CRD bump later.
type BackupThrottle struct {
	MaxBandwidthMbps int32 `json:"maxBandwidthMbps,omitempty"`
}

// NebulaBackupScheduleStatus tracks live schedule state.
type NebulaBackupScheduleStatus struct {
	ObservedGeneration int64        `json:"observedGeneration,omitempty"`
	LastBackupID       string       `json:"lastBackupId,omitempty"`
	LastBackupAt       *metav1.Time `json:"lastBackupAt,omitempty"`
	NextScheduledAt    *metav1.Time `json:"nextScheduledAt,omitempty"`
	// CronJobName is the name of the generated CronJob; lets users
	// `kubectl describe cronjob/<name>` for the raw schedule view.
	CronJobName string             `json:"cronJobName,omitempty"`
	Conditions  []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=nbs;nebbackupschedule
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef`
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="LastBackup",type=date,JSONPath=`.status.lastBackupAt`
// +kubebuilder:printcolumn:name="Suspended",type=boolean,JSONPath=`.spec.suspend`

type NebulaBackupSchedule struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NebulaBackupScheduleSpec   `json:"spec,omitempty"`
	Status            NebulaBackupScheduleStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NebulaBackupScheduleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NebulaBackupSchedule `json:"items"`
}

// =========================================================================
// NebulaBackup — one CR per attempt; created by the schedule controller.
// =========================================================================

type NebulaBackupSpec struct {
	// +kubebuilder:validation:Required
	ClusterRef string `json:"clusterRef"`
	// ScheduleRef is set when the backup was kicked off by a schedule.
	// Empty for ad-hoc backups (a future on-demand path).
	ScheduleRef string            `json:"scheduleRef,omitempty"`
	Storage     BackupStorageSpec `json:"storage"`
}

type NebulaBackupStatus struct {
	Phase              string             `json:"phase,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	BackupID           string             `json:"backupId,omitempty"`
	StartedAt          *metav1.Time       `json:"startedAt,omitempty"`
	CompletedAt        *metav1.Time       `json:"completedAt,omitempty"`
	ManifestURL        string             `json:"manifestURL,omitempty"`
	SizeBytes          int64              `json:"sizeBytes,omitempty"`
	DurationSeconds    int64              `json:"durationSeconds,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=nbk;nebbackup
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef`
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.scheduleRef`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Size",type=integer,JSONPath=`.status.sizeBytes`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

type NebulaBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NebulaBackupSpec   `json:"spec,omitempty"`
	Status            NebulaBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NebulaBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NebulaBackup `json:"items"`
}

// =========================================================================
// NebulaRestore — restore one backup into a cluster's data dir.
// =========================================================================

type NebulaRestoreSpec struct {
	// +kubebuilder:validation:Required
	SourceBackupID string `json:"sourceBackupId"`
	// +kubebuilder:validation:Required
	TargetClusterRef string             `json:"targetClusterRef"`
	Storage          BackupStorageSpec  `json:"storage"`
	// PITR target — optional; v1 only honors target_time at WAL
	// segment-rotation granularity.
	PITR *RestorePITR `json:"pitr,omitempty"`
}

type RestorePITR struct {
	TargetTime *metav1.Time `json:"targetTime,omitempty"`
}

type NebulaRestoreStatus struct {
	Phase              string             `json:"phase,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	RestoreID          string             `json:"restoreId,omitempty"`
	StartedAt          *metav1.Time       `json:"startedAt,omitempty"`
	CompletedAt        *metav1.Time       `json:"completedAt,omitempty"`
	BytesDownloaded    int64              `json:"bytesDownloaded,omitempty"`
	Message            string             `json:"message,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=nrr;nebrestore
// +kubebuilder:printcolumn:name="Backup",type=string,JSONPath=`.spec.sourceBackupId`
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.targetClusterRef`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

type NebulaRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NebulaRestoreSpec   `json:"spec,omitempty"`
	Status            NebulaRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NebulaRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NebulaRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(
		&NebulaBackupSchedule{}, &NebulaBackupScheduleList{},
		&NebulaBackup{}, &NebulaBackupList{},
		&NebulaRestore{}, &NebulaRestoreList{},
	)
}
