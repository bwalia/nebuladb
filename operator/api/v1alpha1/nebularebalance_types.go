package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RebalanceType enumerates the coordination flows this operator can drive.
//
// Standard  — simple scale event: adjust follower count, wait for followers
//
//	to catch up via the leader's WAL tail. Fully supported.
//
// Failover  — leader is unreachable: promote a healthy follower by flipping
//
//	its NEBULA_NODE_ROLE env from "follower" to "leader" on the
//	follower StatefulSet and detaching it from the failed leader.
//	Fully supported given the operator-managed topology.
//
// Swap      — paired old/new pod data migration. Requires a swap rebalance
//
//	engine in NebulaDB which does NOT exist today. The controller
//	accepts the CR, transitions to NotImplemented, and emits a
//	warning event. Kept for forward compatibility.
//
// +kubebuilder:validation:Enum=Standard;Failover;Swap
type RebalanceType string

const (
	RebalanceStandard RebalanceType = "Standard"
	RebalanceFailover RebalanceType = "Failover"
	RebalanceSwap     RebalanceType = "Swap"
)

// RebalancePhase is the state-machine position for this CR.
// +kubebuilder:validation:Enum=Pending;Preflight;Snapshotting;Rebalancing;Promoting;Draining;Completed;Failed;NotImplemented
type RebalancePhase string

const (
	RebalancePending        RebalancePhase = "Pending"
	RebalancePreflight      RebalancePhase = "Preflight"
	RebalanceSnapshotting   RebalancePhase = "Snapshotting"
	RebalanceRebalancing    RebalancePhase = "Rebalancing"
	RebalancePromoting      RebalancePhase = "Promoting"
	RebalanceDraining       RebalancePhase = "Draining"
	RebalanceCompleted      RebalancePhase = "Completed"
	RebalanceFailed         RebalancePhase = "Failed"
	RebalanceNotImplemented RebalancePhase = "NotImplemented"
)

// NodeRef names a pod (StatefulSet podname) involved in a rebalance.
type NodeRef struct {
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Role at time the CR was applied: leader | follower | new.
	Role string `json:"role,omitempty"`
}

// NebulaRebalanceSpec is the desired rebalance operation.
type NebulaRebalanceSpec struct {
	// ClusterRef names the NebulaCluster in the same namespace.
	// +kubebuilder:validation:Required
	ClusterRef string `json:"clusterRef"`

	// Type picks the flow: Standard, Failover, or Swap.
	// +kubebuilder:validation:Required
	Type RebalanceType `json:"type"`

	// Nodes lists the pods to coordinate (for Failover / Swap). For Standard
	// rebalance this field is optional and usually left empty.
	Nodes []NodeRef `json:"nodes,omitempty"`

	// TargetFollowers only applies to Type=Standard and is an alias for a
	// one-shot follower count change. When set, it updates the parent cluster's
	// replication.followers value atomically.
	TargetFollowers *int32 `json:"targetFollowers,omitempty"`

	// Buckets, required for Type=Swap, lists the bucket names to migrate
	// from the "old" node to the "new" node. Empty means all buckets.
	// Each bucket is exported from the source, imported into the target,
	// then emptied on the source once confirmed.
	Buckets []string `json:"buckets,omitempty"`

	// SnapshotBefore runs POST /admin/snapshot before mutating topology.
	// +kubebuilder:default=true
	SnapshotBefore bool `json:"snapshotBefore,omitempty"`

	// CompactAfter runs POST /admin/wal/compact after the rebalance completes.
	// +kubebuilder:default=true
	CompactAfter bool `json:"compactAfter,omitempty"`

	// TimeoutSeconds for the whole operation.
	// +kubebuilder:default=1800
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
}

// NebulaRebalanceStatus tracks state-machine progress.
type NebulaRebalanceStatus struct {
	Phase              RebalancePhase     `json:"phase,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Message            string             `json:"message,omitempty"`
	StartedAt          *metav1.Time       `json:"startedAt,omitempty"`
	CompletedAt        *metav1.Time       `json:"completedAt,omitempty"`
	ProgressPct        int32              `json:"progressPct,omitempty"`
	Steps              []RebalanceStep    `json:"steps,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// RebalanceStep is one unit of work. Kept as a short audit log for debugging.
type RebalanceStep struct {
	Name       string       `json:"name"`
	State      string       `json:"state"`
	Message    string       `json:"message,omitempty"`
	StartedAt  *metav1.Time `json:"startedAt,omitempty"`
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=nr;nebrebalance
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef`
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Progress",type=integer,JSONPath=`.status.progressPct`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NebulaRebalance is the Schema for the nebularebalances API.
type NebulaRebalance struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NebulaRebalanceSpec   `json:"spec,omitempty"`
	Status NebulaRebalanceStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NebulaRebalanceList contains a list of NebulaRebalance.
type NebulaRebalanceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NebulaRebalance `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NebulaRebalance{}, &NebulaRebalanceList{})
}
