// Hand-written deepcopies for the backup CRDs. See
// `zz_generated_deepcopy.go` for the rationale on hand-writing these
// (no controller-gen in CI yet).

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ---------- S3StorageSpec / LocalStorageSpec ----------

func (in *S3StorageSpec) DeepCopyInto(out *S3StorageSpec) {
	*out = *in
	if in.CredentialsSecretRef != nil {
		out.CredentialsSecretRef = in.CredentialsSecretRef.DeepCopy()
	}
}
func (in *S3StorageSpec) DeepCopy() *S3StorageSpec {
	if in == nil {
		return nil
	}
	out := new(S3StorageSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *LocalStorageSpec) DeepCopyInto(out *LocalStorageSpec) { *out = *in }
func (in *LocalStorageSpec) DeepCopy() *LocalStorageSpec {
	if in == nil {
		return nil
	}
	out := new(LocalStorageSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *BackupStorageSpec) DeepCopyInto(out *BackupStorageSpec) {
	*out = *in
	if in.S3 != nil {
		out.S3 = in.S3.DeepCopy()
	}
	if in.Local != nil {
		out.Local = in.Local.DeepCopy()
	}
}

func (in *RetentionPolicy) DeepCopyInto(out *RetentionPolicy) { *out = *in }

func (in *BackupThrottle) DeepCopyInto(out *BackupThrottle) { *out = *in }
func (in *BackupThrottle) DeepCopy() *BackupThrottle {
	if in == nil {
		return nil
	}
	out := new(BackupThrottle)
	in.DeepCopyInto(out)
	return out
}

// ---------- NebulaBackupSchedule ----------

func (in *NebulaBackupScheduleSpec) DeepCopyInto(out *NebulaBackupScheduleSpec) {
	*out = *in
	in.Storage.DeepCopyInto(&out.Storage)
	in.Retention.DeepCopyInto(&out.Retention)
	if in.Throttle != nil {
		out.Throttle = in.Throttle.DeepCopy()
	}
	in.Resources.DeepCopyInto(&out.Resources)
}

func (in *NebulaBackupScheduleStatus) DeepCopyInto(out *NebulaBackupScheduleStatus) {
	*out = *in
	if in.LastBackupAt != nil {
		out.LastBackupAt = in.LastBackupAt.DeepCopy()
	}
	if in.NextScheduledAt != nil {
		out.NextScheduledAt = in.NextScheduledAt.DeepCopy()
	}
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		for i := range in.Conditions {
			in.Conditions[i].DeepCopyInto(&out.Conditions[i])
		}
	}
}

func (in *NebulaBackupSchedule) DeepCopyInto(out *NebulaBackupSchedule) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}
func (in *NebulaBackupSchedule) DeepCopy() *NebulaBackupSchedule {
	if in == nil {
		return nil
	}
	out := new(NebulaBackupSchedule)
	in.DeepCopyInto(out)
	return out
}
func (in *NebulaBackupSchedule) DeepCopyObject() runtime.Object { return in.DeepCopy() }

func (in *NebulaBackupScheduleList) DeepCopyInto(out *NebulaBackupScheduleList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]NebulaBackupSchedule, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}
func (in *NebulaBackupScheduleList) DeepCopy() *NebulaBackupScheduleList {
	if in == nil {
		return nil
	}
	out := new(NebulaBackupScheduleList)
	in.DeepCopyInto(out)
	return out
}
func (in *NebulaBackupScheduleList) DeepCopyObject() runtime.Object { return in.DeepCopy() }

// ---------- NebulaBackup ----------

func (in *NebulaBackupSpec) DeepCopyInto(out *NebulaBackupSpec) {
	*out = *in
	in.Storage.DeepCopyInto(&out.Storage)
}

func (in *NebulaBackupStatus) DeepCopyInto(out *NebulaBackupStatus) {
	*out = *in
	if in.StartedAt != nil {
		out.StartedAt = in.StartedAt.DeepCopy()
	}
	if in.CompletedAt != nil {
		out.CompletedAt = in.CompletedAt.DeepCopy()
	}
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		for i := range in.Conditions {
			in.Conditions[i].DeepCopyInto(&out.Conditions[i])
		}
	}
}

func (in *NebulaBackup) DeepCopyInto(out *NebulaBackup) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}
func (in *NebulaBackup) DeepCopy() *NebulaBackup {
	if in == nil {
		return nil
	}
	out := new(NebulaBackup)
	in.DeepCopyInto(out)
	return out
}
func (in *NebulaBackup) DeepCopyObject() runtime.Object { return in.DeepCopy() }

func (in *NebulaBackupList) DeepCopyInto(out *NebulaBackupList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]NebulaBackup, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}
func (in *NebulaBackupList) DeepCopy() *NebulaBackupList {
	if in == nil {
		return nil
	}
	out := new(NebulaBackupList)
	in.DeepCopyInto(out)
	return out
}
func (in *NebulaBackupList) DeepCopyObject() runtime.Object { return in.DeepCopy() }

// ---------- NebulaRestore ----------

func (in *NebulaRestoreSpec) DeepCopyInto(out *NebulaRestoreSpec) {
	*out = *in
	in.Storage.DeepCopyInto(&out.Storage)
	if in.PITR != nil {
		pitr := *in.PITR
		if pitr.TargetTime != nil {
			pitr.TargetTime = in.PITR.TargetTime.DeepCopy()
		}
		out.PITR = &pitr
	}
}

func (in *NebulaRestoreStatus) DeepCopyInto(out *NebulaRestoreStatus) {
	*out = *in
	if in.StartedAt != nil {
		out.StartedAt = in.StartedAt.DeepCopy()
	}
	if in.CompletedAt != nil {
		out.CompletedAt = in.CompletedAt.DeepCopy()
	}
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		for i := range in.Conditions {
			in.Conditions[i].DeepCopyInto(&out.Conditions[i])
		}
	}
}

func (in *NebulaRestore) DeepCopyInto(out *NebulaRestore) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}
func (in *NebulaRestore) DeepCopy() *NebulaRestore {
	if in == nil {
		return nil
	}
	out := new(NebulaRestore)
	in.DeepCopyInto(out)
	return out
}
func (in *NebulaRestore) DeepCopyObject() runtime.Object { return in.DeepCopy() }

func (in *NebulaRestoreList) DeepCopyInto(out *NebulaRestoreList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]NebulaRestore, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}
func (in *NebulaRestoreList) DeepCopy() *NebulaRestoreList {
	if in == nil {
		return nil
	}
	out := new(NebulaRestoreList)
	in.DeepCopyInto(out)
	return out
}
func (in *NebulaRestoreList) DeepCopyObject() runtime.Object { return in.DeepCopy() }

// Force a corev1 reference so go imports stay tidy; the spec uses
// corev1.ResourceRequirements directly above, but the import would
// be flagged unused if no symbol referenced it here.
var _ = corev1.ResourceRequirements{}
