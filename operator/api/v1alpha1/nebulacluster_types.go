package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// ClusterPhase is the lifecycle phase reported in Status.
// +kubebuilder:validation:Enum=Pending;Provisioning;Running;Degraded;Rebalancing;Upgrading;Failed
type ClusterPhase string

const (
	PhasePending      ClusterPhase = "Pending"
	PhaseProvisioning ClusterPhase = "Provisioning"
	PhaseRunning      ClusterPhase = "Running"
	PhaseDegraded     ClusterPhase = "Degraded"
	PhaseRebalancing  ClusterPhase = "Rebalancing"
	PhaseUpgrading    ClusterPhase = "Upgrading"
	PhaseFailed       ClusterPhase = "Failed"
)

// StorageSpec pins the leader/follower persistent volume shape.
type StorageSpec struct {
	// +kubebuilder:default="10Gi"
	Size string `json:"size,omitempty"`

	// StorageClassName for the PVC. Empty string means the cluster default.
	StorageClassName *string `json:"storageClassName,omitempty"`

	// AccessModes defaults to ReadWriteOnce.
	AccessModes []corev1.PersistentVolumeAccessMode `json:"accessModes,omitempty"`
}

// AIConfig toggles embedding + LLM integration env vars on every pod.
type AIConfig struct {
	// +kubebuilder:default=true
	Enabled bool `json:"enabled,omitempty"`

	// EmbeddingModel sets NEBULA_OPENAI_MODEL. Ignored if Enabled=false.
	EmbeddingModel string `json:"embeddingModel,omitempty"`

	// EmbeddingDim sets NEBULA_EMBED_DIM. Must match the model.
	EmbeddingDim int32 `json:"embeddingDim,omitempty"`

	// OllamaURL sets NEBULA_LLM_OLLAMA_URL for local model serving.
	OllamaURL string `json:"ollamaURL,omitempty"`

	// LLMModel sets NEBULA_LLM_MODEL. Defaults resolved in-binary.
	LLMModel string `json:"llmModel,omitempty"`

	// OpenAIAPIKeySecretRef points at a Secret holding NEBULA_OPENAI_API_KEY.
	OpenAIAPIKeySecretRef *SecretKeySelector `json:"openAIAPIKeySecretRef,omitempty"`

	// LLMOpenAIKeySecretRef points at a Secret holding NEBULA_LLM_OPENAI_KEY.
	LLMOpenAIKeySecretRef *SecretKeySelector `json:"llmOpenAIKeySecretRef,omitempty"`
}

// SecretKeySelector selects a key from a Kubernetes Secret.
type SecretKeySelector struct {
	// +kubebuilder:validation:Required
	Name string `json:"name"`
	// +kubebuilder:validation:Required
	Key string `json:"key"`
}

// ReplicationSpec controls follower replication.
type ReplicationSpec struct {
	// Enabled provisions a follower StatefulSet when true.
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// Followers is the desired follower replica count.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	Followers int32 `json:"followers,omitempty"`

	// RestartPolicyOnLag triggers a follower restart if lag crosses the threshold.
	// +kubebuilder:default=false
	RestartPolicyOnLag bool `json:"restartPolicyOnLag,omitempty"`

	// LagByteThreshold bytes above which an automatic action (alert/restart) fires.
	LagByteThreshold int64 `json:"lagByteThreshold,omitempty"`
}

// RegionSpec describes a deployment zone. Multi-region is application-routed —
// the operator provisions independent clusters per region.
type RegionSpec struct {
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// NodeSelector targets nodes in this region.
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations attached to pods for this region.
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// TopologySpreadConstraints to pin pods to zones/racks within the region.
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
}

// AutoscalingSpec drives HPA-style follower scaling. Leader is always 1.
type AutoscalingSpec struct {
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// +kubebuilder:validation:Minimum=1
	MinFollowers int32 `json:"minFollowers,omitempty"`

	// +kubebuilder:validation:Minimum=1
	MaxFollowers int32 `json:"maxFollowers,omitempty"`

	// TargetCPUUtilization percent.
	TargetCPUUtilization *int32 `json:"targetCPUUtilization,omitempty"`

	// TargetQPS scales followers when searches-per-second crosses this mark.
	TargetQPS *int32 `json:"targetQPS,omitempty"`

	// TargetStorageUtilization percent of requested PVC size.
	TargetStorageUtilization *int32 `json:"targetStorageUtilization,omitempty"`
}

// BackupSpec enables scheduled snapshots via POST /admin/snapshot.
type BackupSpec struct {
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// Schedule in cron format. Defaults to "0 */6 * * *" (every 6h).
	Schedule string `json:"schedule,omitempty"`

	// Retain N most recent snapshots. 0 means keep all.
	// +kubebuilder:default=14
	Retain int32 `json:"retain,omitempty"`

	// TargetVolumeClaimName optionally writes snapshots to a different PVC
	// than the data volume. Leave empty to store alongside WAL.
	TargetVolumeClaimName string `json:"targetVolumeClaimName,omitempty"`
}

// SecuritySpec covers auth env injection.
type SecuritySpec struct {
	// APIKeysSecretRef is a Secret with key "keys" containing comma-separated
	// bearer tokens for NEBULA_API_KEYS.
	APIKeysSecretRef *SecretKeySelector `json:"apiKeysSecretRef,omitempty"`

	// JWTSecretRef points at the HS256 secret for NEBULA_JWT_SECRET.
	JWTSecretRef *SecretKeySelector `json:"jwtSecretRef,omitempty"`

	// JWTIssuer sets NEBULA_JWT_ISS.
	JWTIssuer string `json:"jwtIssuer,omitempty"`

	// JWTAudience sets NEBULA_JWT_AUD.
	JWTAudience string `json:"jwtAudience,omitempty"`

	// TLSSecretRef configures an optional TLS-terminating sidecar.
	TLSSecretRef *corev1.LocalObjectReference `json:"tlsSecretRef,omitempty"`
}

// UpgradeStrategy describes how Reconciler should advance Version.
// +kubebuilder:validation:Enum=Recreate;SwapRebalance
type UpgradeStrategy string

const (
	// UpgradeRecreate is the supported path today: snapshot leader, Recreate
	// pod, wait for followers to reconcile.
	UpgradeRecreate UpgradeStrategy = "Recreate"

	// UpgradeSwapRebalance is aspirational — requires a swap rebalance engine
	// which does not yet exist in NebulaDB. Operator logs "not implemented"
	// and falls back to Recreate.
	UpgradeSwapRebalance UpgradeStrategy = "SwapRebalance"
)

// NebulaClusterSpec defines the desired state of NebulaCluster.
type NebulaClusterSpec struct {
	// Version is the container image tag. Example: "0.1.0".
	// +kubebuilder:validation:Required
	Version string `json:"version"`

	// Image repository. Default: ghcr.io/bwalia/nebuladb.
	// +kubebuilder:default="ghcr.io/bwalia/nebuladb"
	Image string `json:"image,omitempty"`

	// ImagePullPolicy. Default IfNotPresent.
	// +kubebuilder:default=IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSecrets for private registries.
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// Storage configuration for both leader and followers.
	Storage StorageSpec `json:"storage,omitempty"`

	// Resources for every nebula container.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Replication toggle + follower count.
	Replication ReplicationSpec `json:"replication,omitempty"`

	// AI/RAG configuration.
	AI AIConfig `json:"ai,omitempty"`

	// Autoscaling config for followers.
	Autoscaling AutoscalingSpec `json:"autoscaling,omitempty"`

	// Backup + snapshot retention.
	Backup BackupSpec `json:"backup,omitempty"`

	// Security + auth.
	Security SecuritySpec `json:"security,omitempty"`

	// Regions — multi-region is app-level routing. Each region gets its own
	// StatefulSet (leader+followers) scoped to the nodeSelector/tolerations.
	// When empty, a single region "default" is synthesized.
	Regions []RegionSpec `json:"regions,omitempty"`

	// UpgradeStrategy defaults to Recreate.
	// +kubebuilder:default=Recreate
	UpgradeStrategy UpgradeStrategy `json:"upgradeStrategy,omitempty"`

	// MaxUnavailable during reconciliation.
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`

	// ExtraEnv passthrough to every pod. Useful for NEBULA_RATE_LIMIT tuning.
	ExtraEnv []corev1.EnvVar `json:"extraEnv,omitempty"`

	// ServiceType for the leader and follower services.
	// +kubebuilder:default=ClusterIP
	ServiceType corev1.ServiceType `json:"serviceType,omitempty"`

	// PodAnnotations merged onto every pod.
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`

	// PodLabels merged onto every pod.
	PodLabels map[string]string `json:"podLabels,omitempty"`
}

// NodeStatus is a per-pod health snapshot sampled by the controller.
type NodeStatus struct {
	Name          string `json:"name"`
	Role          string `json:"role"`
	Region        string `json:"region,omitempty"`
	Address       string `json:"address,omitempty"`
	Healthy       bool   `json:"healthy"`
	Docs          int64  `json:"docs,omitempty"`
	Version       string `json:"version,omitempty"`
	LagBytes      *int64 `json:"lagBytes,omitempty"`
	LastProbeTime string `json:"lastProbeTime,omitempty"`
}

// RebalanceState is the sub-resource status mirrored from an active
// NebulaRebalance object, if any.
type RebalanceState struct {
	Active       bool   `json:"active"`
	Type         string `json:"type,omitempty"`
	Phase        string `json:"phase,omitempty"`
	ProgressPct  int32  `json:"progressPct,omitempty"`
	RebalanceRef string `json:"rebalanceRef,omitempty"`
}

// NebulaClusterStatus defines the observed state of NebulaCluster.
type NebulaClusterStatus struct {
	Phase              ClusterPhase       `json:"phase,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	CurrentVersion     string             `json:"currentVersion,omitempty"`
	Leader             *NodeStatus        `json:"leader,omitempty"`
	Followers          []NodeStatus       `json:"followers,omitempty"`
	ReadyReplicas      int32              `json:"readyReplicas,omitempty"`
	DesiredReplicas    int32              `json:"desiredReplicas,omitempty"`
	Rebalance          *RebalanceState    `json:"rebalance,omitempty"`
	LastSnapshotAt     *metav1.Time       `json:"lastSnapshotAt,omitempty"`
	LastSnapshotPath   string             `json:"lastSnapshotPath,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=nc;nebcluster
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.status.currentVersion`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.status.desiredReplicas`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NebulaCluster is the Schema for the nebulaclusters API.
type NebulaCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NebulaClusterSpec   `json:"spec,omitempty"`
	Status NebulaClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NebulaClusterList contains a list of NebulaCluster.
type NebulaClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NebulaCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NebulaCluster{}, &NebulaClusterList{})
}
