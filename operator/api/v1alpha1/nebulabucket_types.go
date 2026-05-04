package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BucketType is a hint for downstream clients — NebulaDB buckets are
// schemaless, so this affects operator behavior only (metadata, seeding),
// not server-side validation.
// +kubebuilder:validation:Enum=SQL;JSON;Hybrid;Vector
type BucketType string

const (
	BucketSQL    BucketType = "SQL"
	BucketJSON   BucketType = "JSON"
	BucketHybrid BucketType = "Hybrid"
	BucketVector BucketType = "Vector"
)

// BucketReplicationPolicy is a forward-looking knob. Today it is cosmetic:
// NebulaDB replication is cluster-wide via the leader/follower gRPC tail.
// The operator reads this to emit warnings if it conflicts with the cluster's
// replication spec.
type BucketReplicationPolicy struct {
	// Mode: "cluster-wide" (default) or "local" (per-region only).
	// +kubebuilder:validation:Enum=cluster-wide;local
	// +kubebuilder:default=cluster-wide
	Mode string `json:"mode,omitempty"`

	// MinReplicas the operator will warn about if followers drop below this.
	MinReplicas int32 `json:"minReplicas,omitempty"`
}

// BucketIndexingSpec describes the indexing configuration hints that the
// operator writes into the bucket's seed metadata document.
type BucketIndexingSpec struct {
	// ChunkChars overrides NEBULA_CHUNK_CHARS for documents upserted through
	// /bucket/:bucket/document. The operator records the intent but does not
	// rewrite env vars at pod level (those are per-process).
	ChunkChars int32 `json:"chunkChars,omitempty"`

	// ChunkOverlap overrides NEBULA_CHUNK_OVERLAP.
	ChunkOverlap int32 `json:"chunkOverlap,omitempty"`

	// Metric is a documentation-only hint; HNSW today is always cosine.
	// +kubebuilder:validation:Enum=cosine;dot;l2
	// +kubebuilder:default=cosine
	Metric string `json:"metric,omitempty"`
}

// NebulaBucketSpec declaratively manages a bucket in a NebulaCluster.
type NebulaBucketSpec struct {
	// ClusterRef is the name of the NebulaCluster (same namespace) that owns
	// this bucket.
	// +kubebuilder:validation:Required
	ClusterRef string `json:"clusterRef"`

	// Name is the bucket name as stored in NebulaDB. Defaults to
	// metadata.name if empty.
	Name string `json:"name,omitempty"`

	// Type is a hint for tooling. Buckets are schemaless.
	// +kubebuilder:default=Hybrid
	Type BucketType `json:"type,omitempty"`

	// Replication policy hint (cosmetic — see BucketReplicationPolicy).
	Replication BucketReplicationPolicy `json:"replication,omitempty"`

	// Indexing hints.
	Indexing BucketIndexingSpec `json:"indexing,omitempty"`

	// DeletePolicy controls what happens when the CR is deleted.
	// "Retain" keeps the bucket contents; "Empty" calls POST /admin/bucket/:b/empty.
	// +kubebuilder:validation:Enum=Retain;Empty
	// +kubebuilder:default=Retain
	DeletePolicy string `json:"deletePolicy,omitempty"`

	// Labels recorded in the seed document's metadata.
	Labels map[string]string `json:"labels,omitempty"`

	// HomeRegion is the region that authoritatively accepts writes for
	// this bucket. When unset, the bucket has no home and writes land
	// wherever the client hits (the legacy behavior). The bucket
	// controller writes this to the seed doc's home_region field.
	HomeRegion string `json:"homeRegion,omitempty"`

	// ReplicatedTo is the list of non-home regions that receive
	// cross-region WAL tail for this bucket. Informational — the
	// cross-region plumbing is driven by the cluster's crossRegion
	// peers, not this field, but having it in the seed doc lets the
	// nebula-client SDK surface "where can I safely read?".
	ReplicatedTo []string `json:"replicatedTo,omitempty"`
}

// NebulaBucketStatus tracks the live state of a bucket.
type NebulaBucketStatus struct {
	Phase              string       `json:"phase,omitempty"`
	ObservedGeneration int64        `json:"observedGeneration,omitempty"`
	DocCount           int64        `json:"docCount,omitempty"`
	ParentDocs         int64        `json:"parentDocs,omitempty"`
	TopKeys            []string     `json:"topKeys,omitempty"`
	LastSeeded         *metav1.Time `json:"lastSeeded,omitempty"`
	// CurrentHomeRegion is the region currently serving writes for
	// this bucket — usually equals spec.homeRegion but can diverge
	// during an in-progress failover.
	CurrentHomeRegion string `json:"currentHomeRegion,omitempty"`
	// HomeEpoch is the last epoch the controller observed. Only the
	// NebulaRegionFailover controller bumps this — creation seeds
	// it to 1.
	HomeEpoch  int64              `json:"homeEpoch,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=nb;nebbucket
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef`
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Docs",type=integer,JSONPath=`.status.docCount`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NebulaBucket is the Schema for the nebulabuckets API.
type NebulaBucket struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NebulaBucketSpec   `json:"spec,omitempty"`
	Status NebulaBucketStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NebulaBucketList contains a list of NebulaBucket.
type NebulaBucketList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NebulaBucket `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NebulaBucket{}, &NebulaBucketList{})
}
