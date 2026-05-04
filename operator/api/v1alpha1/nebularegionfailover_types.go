package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RegionFailoverPhase is the state-machine position for a failover.
// +kubebuilder:validation:Enum=Pending;Preflight;Draining;EpochBump;Announce;Completed;Failed
type RegionFailoverPhase string

const (
	RegionFailoverPending   RegionFailoverPhase = "Pending"
	RegionFailoverPreflight RegionFailoverPhase = "Preflight"
	RegionFailoverDraining  RegionFailoverPhase = "Draining"
	RegionFailoverEpochBump RegionFailoverPhase = "EpochBump"
	RegionFailoverAnnounce  RegionFailoverPhase = "Announce"
	RegionFailoverCompleted RegionFailoverPhase = "Completed"
	RegionFailoverFailed    RegionFailoverPhase = "Failed"
)

// NebulaRegionFailoverSpec rebinds a bucket's home_region to a new
// region. Failover is single-fire: once Completed or Failed, the CR
// is a historical record and no further reconciliation happens.
type NebulaRegionFailoverSpec struct {
	// ClusterRef names the NebulaCluster in this namespace.
	// +kubebuilder:validation:Required
	ClusterRef string `json:"clusterRef"`

	// Bucket to rebind.
	// +kubebuilder:validation:Required
	Bucket string `json:"bucket"`

	// NewHomeRegion is the region that should become authoritative
	// after this failover completes.
	// +kubebuilder:validation:Required
	NewHomeRegion string `json:"newHomeRegion"`

	// Force skips the "current home is unreachable" check. Set this
	// for planned failovers where the old home is still healthy —
	// emergency paths should leave it false so the controller
	// verifies the old home is actually dead before rebinding.
	Force bool `json:"force,omitempty"`

	// DrainTimeoutSeconds is how long the controller waits for the
	// target region's data to catch up via swap rebalance. Past this,
	// the controller either fails or (with Force=true) proceeds with
	// whatever data the target has.
	// +kubebuilder:default=600
	DrainTimeoutSeconds int32 `json:"drainTimeoutSeconds,omitempty"`

	// TimeoutSeconds is an overall upper bound on the whole operation.
	// +kubebuilder:default=1800
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
}

// NebulaRegionFailoverStatus tracks state-machine progress.
type NebulaRegionFailoverStatus struct {
	Phase              RegionFailoverPhase `json:"phase,omitempty"`
	ObservedGeneration int64               `json:"observedGeneration,omitempty"`
	Message            string              `json:"message,omitempty"`
	StartedAt          *metav1.Time        `json:"startedAt,omitempty"`
	CompletedAt        *metav1.Time        `json:"completedAt,omitempty"`
	// FromHomeRegion is the home at the moment the CR was accepted.
	FromHomeRegion string `json:"fromHomeRegion,omitempty"`
	// FromEpoch is the epoch we read on admission; used as the base
	// for the bump so races against concurrent failovers fail loudly.
	FromEpoch int64 `json:"fromEpoch,omitempty"`
	// ToEpoch is the epoch written on success. Equals FromEpoch + 1
	// for normal operation; larger jumps indicate the controller
	// observed a concurrent writer and re-read mid-flight.
	ToEpoch int64 `json:"toEpoch,omitempty"`
	// Steps is an audit log mirroring NebulaRebalance for consistency.
	Steps      []RebalanceStep    `json:"steps,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=nrf;nebfailover
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef`
// +kubebuilder:printcolumn:name="Bucket",type=string,JSONPath=`.spec.bucket`
// +kubebuilder:printcolumn:name="From",type=string,JSONPath=`.status.fromHomeRegion`
// +kubebuilder:printcolumn:name="To",type=string,JSONPath=`.spec.newHomeRegion`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NebulaRegionFailover is the Schema for the nebularegionfailovers API.
type NebulaRegionFailover struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NebulaRegionFailoverSpec   `json:"spec,omitempty"`
	Status NebulaRegionFailoverStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NebulaRegionFailoverList contains a list of NebulaRegionFailover.
type NebulaRegionFailoverList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NebulaRegionFailover `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NebulaRegionFailover{}, &NebulaRegionFailoverList{})
}
