package controllers

import (
	nebulav1alpha1 "github.com/bwalia/nebuladb-operator/api/v1alpha1"
)

// Label keys the operator owns. Kept as constants so they can be reused by
// all three controllers without typo drift.
const (
	LabelAppName      = "app.kubernetes.io/name"
	LabelAppInstance  = "app.kubernetes.io/instance"
	LabelAppComponent = "app.kubernetes.io/component"
	LabelAppVersion   = "app.kubernetes.io/version"
	LabelAppManagedBy = "app.kubernetes.io/managed-by"

	LabelClusterName = "nebula.nebuladb.io/cluster"
	LabelRegion      = "nebula.nebuladb.io/region"
	LabelRole        = "nebula.nebuladb.io/role"

	ComponentLeader   = "leader"
	ComponentFollower = "follower"

	ManagedBy = "nebuladb-operator"
	AppName   = "nebuladb"

	FinalizerCluster   = "nebula.nebuladb.io/cluster-finalizer"
	FinalizerBucket    = "nebula.nebuladb.io/bucket-finalizer"
	FinalizerRebalance = "nebula.nebuladb.io/rebalance-finalizer"
)

// baseLabels returns the common identity labels for a given cluster + role +
// region. Merged with user-supplied PodLabels when building pod templates.
func baseLabels(cluster *nebulav1alpha1.NebulaCluster, role, region string) map[string]string {
	l := map[string]string{
		LabelAppName:      AppName,
		LabelAppInstance:  cluster.Name,
		LabelAppComponent: role,
		LabelAppVersion:   cluster.Spec.Version,
		LabelAppManagedBy: ManagedBy,
		LabelClusterName:  cluster.Name,
		LabelRole:         role,
	}
	if region != "" {
		l[LabelRegion] = region
	}
	return l
}

// selectorLabels are the subset used in Service selectors + StatefulSet
// matchLabels. They must stay immutable across spec edits, so version is
// deliberately excluded.
func selectorLabels(clusterName, role, region string) map[string]string {
	l := map[string]string{
		LabelAppName:      AppName,
		LabelAppInstance:  clusterName,
		LabelAppComponent: role,
		LabelClusterName:  clusterName,
		LabelRole:         role,
	}
	if region != "" {
		l[LabelRegion] = region
	}
	return l
}

// mergeMaps returns a new map with b layered over a. Neither input is mutated.
func mergeMaps(a, b map[string]string) map[string]string {
	out := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// regionsOrDefault returns the configured regions, or a single synthesized
// "default" region when the user has not specified any.
func regionsOrDefault(cluster *nebulav1alpha1.NebulaCluster) []nebulav1alpha1.RegionSpec {
	if len(cluster.Spec.Regions) > 0 {
		return cluster.Spec.Regions
	}
	return []nebulav1alpha1.RegionSpec{{Name: "default"}}
}
