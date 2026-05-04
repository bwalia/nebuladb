package controllers

import (
	"fmt"
	"strconv"

	nebulav1alpha1 "github.com/bwalia/nebuladb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Resource naming. NebulaDB is single-writer-per-node, so the leader always
// runs in its own StatefulSet of size 1. Followers run in a separate STS per
// region so they can scale independently.

func leaderSTSName(cluster *nebulav1alpha1.NebulaCluster, region string) string {
	if region == "" || region == "default" {
		return fmt.Sprintf("%s-leader", cluster.Name)
	}
	return fmt.Sprintf("%s-%s-leader", cluster.Name, region)
}

func followerSTSName(cluster *nebulav1alpha1.NebulaCluster, region string) string {
	if region == "" || region == "default" {
		return fmt.Sprintf("%s-follower", cluster.Name)
	}
	return fmt.Sprintf("%s-%s-follower", cluster.Name, region)
}

func leaderServiceName(cluster *nebulav1alpha1.NebulaCluster, region string) string {
	if region == "" || region == "default" {
		return fmt.Sprintf("%s-leader", cluster.Name)
	}
	return fmt.Sprintf("%s-%s-leader", cluster.Name, region)
}

func followerServiceName(cluster *nebulav1alpha1.NebulaCluster, region string) string {
	if region == "" || region == "default" {
		return fmt.Sprintf("%s-follower", cluster.Name)
	}
	return fmt.Sprintf("%s-%s-follower", cluster.Name, region)
}

func headlessServiceName(cluster *nebulav1alpha1.NebulaCluster, region, role string) string {
	name := cluster.Name
	if region != "" && region != "default" {
		name = fmt.Sprintf("%s-%s", cluster.Name, region)
	}
	return fmt.Sprintf("%s-%s-headless", name, role)
}

// leaderGRPCDNS returns the in-cluster address a follower uses for
// NEBULA_FOLLOW_LEADER. Must match the leader headless service + pod 0.
func leaderGRPCDNS(cluster *nebulav1alpha1.NebulaCluster, region, namespace string) string {
	svc := headlessServiceName(cluster, region, ComponentLeader)
	sts := leaderSTSName(cluster, region)
	return fmt.Sprintf("http://%s-0.%s.%s.svc.cluster.local:50051", sts, svc, namespace)
}

// leaderRESTDNS returns the in-cluster REST URL for lag probing.
func leaderRESTDNS(cluster *nebulav1alpha1.NebulaCluster, region, namespace string) string {
	svc := headlessServiceName(cluster, region, ComponentLeader)
	sts := leaderSTSName(cluster, region)
	return fmt.Sprintf("http://%s-0.%s.%s.svc.cluster.local:8080", sts, svc, namespace)
}

// buildPodTemplate assembles the shared pod template. role is "leader" or
// "follower". region lets us inject region-specific node selectors.
func buildPodTemplate(cluster *nebulav1alpha1.NebulaCluster, region nebulav1alpha1.RegionSpec, role string) corev1.PodTemplateSpec {
	labels := mergeMaps(baseLabels(cluster, role, region.Name), cluster.Spec.PodLabels)
	anns := mergeMaps(map[string]string{
		"prometheus.io/scrape": "true",
		"prometheus.io/port":   "8080",
		"prometheus.io/path":   "/metrics",
	}, cluster.Spec.PodAnnotations)

	env := buildEnv(cluster, region, role)

	resources := cluster.Spec.Resources
	if resources.Requests == nil {
		resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("200m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		}
	}
	if resources.Limits == nil {
		resources.Limits = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("2Gi"),
		}
	}

	container := corev1.Container{
		Name:            "nebula",
		Image:           fmt.Sprintf("%s:%s", cluster.Spec.Image, cluster.Spec.Version),
		ImagePullPolicy: cluster.Spec.ImagePullPolicy,
		Ports: []corev1.ContainerPort{
			{Name: "rest", ContainerPort: 8080},
			{Name: "grpc", ContainerPort: 50051},
			{Name: "pg", ContainerPort: 5432},
		},
		Env:       env,
		Resources: resources,
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intOrStr("rest")},
			},
			InitialDelaySeconds: 2,
			PeriodSeconds:       5,
			FailureThreshold:    6,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intOrStr("rest")},
			},
			InitialDelaySeconds: 30,
			PeriodSeconds:       20,
			FailureThreshold:    5,
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: "/var/lib/nebuladb"},
			{Name: "tmp", MountPath: "/tmp"},
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptrBool(false),
			ReadOnlyRootFilesystem:   ptrBool(true),
			RunAsNonRoot:             ptrBool(true),
			RunAsUser:                ptrInt64(10001),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
	}

	spec := corev1.PodSpec{
		Containers: []corev1.Container{container},
		Volumes: []corev1.Volume{
			{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		},
		SecurityContext: &corev1.PodSecurityContext{
			FSGroup:      ptrInt64(10001),
			RunAsNonRoot: ptrBool(true),
		},
		ImagePullSecrets: cluster.Spec.ImagePullSecrets,
		NodeSelector:     region.NodeSelector,
		Tolerations:      region.Tolerations,
	}
	if len(region.TopologySpreadConstraints) > 0 {
		spec.TopologySpreadConstraints = region.TopologySpreadConstraints
	}

	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      labels,
			Annotations: anns,
		},
		Spec: spec,
	}
}

// buildEnv assembles the NEBULA_* env vars. Roles affect the cluster-config
// vars (role, peers, follow) and the listener toggles: followers deliberately
// omit NEBULA_PG_BIND/NEBULA_GRPC_BIND (except where replication needs gRPC
// — but NebulaDB only needs the leader's gRPC surface, not the follower's).
func buildEnv(cluster *nebulav1alpha1.NebulaCluster, region nebulav1alpha1.RegionSpec, role string) []corev1.EnvVar {
	env := []corev1.EnvVar{
		{Name: "NEBULA_BIND", Value: "0.0.0.0:8080"},
		{Name: "NEBULA_DATA_DIR", Value: "/var/lib/nebuladb"},
		{Name: "NEBULA_NODE_ID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "NEBULA_NODE_ROLE", Value: role},
	}

	if role == ComponentLeader {
		env = append(env,
			corev1.EnvVar{Name: "NEBULA_GRPC_BIND", Value: "0.0.0.0:50051"},
			corev1.EnvVar{Name: "NEBULA_PG_BIND", Value: "0.0.0.0:5432"},
		)
	}
	if role == ComponentFollower {
		env = append(env,
			corev1.EnvVar{Name: "NEBULA_FOLLOW_LEADER", Value: leaderGRPCDNS(cluster, region.Name, cluster.Namespace)},
			corev1.EnvVar{Name: "NEBULA_LEADER_REST_URL", Value: leaderRESTDNS(cluster, region.Name, cluster.Namespace)},
		)
	}

	// AI/RAG knobs
	if cluster.Spec.AI.Enabled {
		if cluster.Spec.AI.EmbeddingModel != "" {
			env = append(env, corev1.EnvVar{Name: "NEBULA_OPENAI_MODEL", Value: cluster.Spec.AI.EmbeddingModel})
		}
		if cluster.Spec.AI.EmbeddingDim > 0 {
			env = append(env, corev1.EnvVar{Name: "NEBULA_EMBED_DIM", Value: strconv.Itoa(int(cluster.Spec.AI.EmbeddingDim))})
		}
		if cluster.Spec.AI.OllamaURL != "" {
			env = append(env, corev1.EnvVar{Name: "NEBULA_LLM_OLLAMA_URL", Value: cluster.Spec.AI.OllamaURL})
		}
		if cluster.Spec.AI.LLMModel != "" {
			env = append(env, corev1.EnvVar{Name: "NEBULA_LLM_MODEL", Value: cluster.Spec.AI.LLMModel})
		}
		if ref := cluster.Spec.AI.OpenAIAPIKeySecretRef; ref != nil {
			env = append(env, corev1.EnvVar{
				Name: "NEBULA_OPENAI_API_KEY",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: ref.Name},
						Key:                  ref.Key,
					},
				},
			})
		}
		if ref := cluster.Spec.AI.LLMOpenAIKeySecretRef; ref != nil {
			env = append(env, corev1.EnvVar{
				Name: "NEBULA_LLM_OPENAI_KEY",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: ref.Name},
						Key:                  ref.Key,
					},
				},
			})
		}
	}

	// Security: API keys + JWT
	if ref := cluster.Spec.Security.APIKeysSecretRef; ref != nil {
		env = append(env, corev1.EnvVar{
			Name: "NEBULA_API_KEYS",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: ref.Name},
					Key:                  ref.Key,
				},
			},
		})
	}
	if ref := cluster.Spec.Security.JWTSecretRef; ref != nil {
		env = append(env, corev1.EnvVar{
			Name: "NEBULA_JWT_SECRET",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: ref.Name},
					Key:                  ref.Key,
				},
			},
		})
	}
	if cluster.Spec.Security.JWTIssuer != "" {
		env = append(env, corev1.EnvVar{Name: "NEBULA_JWT_ISS", Value: cluster.Spec.Security.JWTIssuer})
	}
	if cluster.Spec.Security.JWTAudience != "" {
		env = append(env, corev1.EnvVar{Name: "NEBULA_JWT_AUD", Value: cluster.Spec.Security.JWTAudience})
	}

	env = append(env, cluster.Spec.ExtraEnv...)
	return env
}

// buildPVCTemplate returns the volumeClaimTemplate entry for a STS.
func buildPVCTemplate(cluster *nebulav1alpha1.NebulaCluster) corev1.PersistentVolumeClaim {
	size := cluster.Spec.Storage.Size
	if size == "" {
		size = "10Gi"
	}
	accessModes := cluster.Spec.Storage.AccessModes
	if len(accessModes) == 0 {
		accessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: accessModes,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(size),
				},
			},
			StorageClassName: cluster.Spec.Storage.StorageClassName,
		},
	}
}

// buildStatefulSet constructs leader (size 1) or follower (size N) STS.
// For the leader we use Recreate-like behavior by setting OnDelete strategy;
// the controller drives the snapshot-before-recreate flow explicitly.
func buildStatefulSet(cluster *nebulav1alpha1.NebulaCluster, region nebulav1alpha1.RegionSpec, role string) *appsv1.StatefulSet {
	var replicas int32
	var name, svcName string
	if role == ComponentLeader {
		replicas = 1
		name = leaderSTSName(cluster, region.Name)
		svcName = headlessServiceName(cluster, region.Name, ComponentLeader)
	} else {
		replicas = cluster.Spec.Replication.Followers
		if !cluster.Spec.Replication.Enabled {
			replicas = 0
		}
		name = followerSTSName(cluster, region.Name)
		svcName = headlessServiceName(cluster, region.Name, ComponentFollower)
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    baseLabels(cluster, role, region.Name),
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: svcName,
			Replicas:    &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: selectorLabels(cluster.Name, role, region.Name),
			},
			Template:             buildPodTemplate(cluster, region, role),
			PodManagementPolicy:  appsv1.ParallelPodManagement,
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{buildPVCTemplate(cluster)},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.OnDeleteStatefulSetStrategyType,
			},
		},
	}
	return sts
}

// buildHeadlessService is the DNS anchor for pod-specific hostnames.
func buildHeadlessService(cluster *nebulav1alpha1.NebulaCluster, region nebulav1alpha1.RegionSpec, role string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      headlessServiceName(cluster, region.Name, role),
			Namespace: cluster.Namespace,
			Labels:    baseLabels(cluster, role, region.Name),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 selectorLabels(cluster.Name, role, region.Name),
			Ports: []corev1.ServicePort{
				{Name: "rest", Port: 8080, TargetPort: intOrStr("rest")},
				{Name: "grpc", Port: 50051, TargetPort: intOrStr("grpc")},
				{Name: "pg", Port: 5432, TargetPort: intOrStr("pg")},
			},
		},
	}
}

// buildClientService is the stable service the workloads target.
func buildClientService(cluster *nebulav1alpha1.NebulaCluster, region nebulav1alpha1.RegionSpec, role string) *corev1.Service {
	name := leaderServiceName(cluster, region.Name)
	if role == ComponentFollower {
		name = followerServiceName(cluster, region.Name)
	}
	typ := cluster.Spec.ServiceType
	if typ == "" {
		typ = corev1.ServiceTypeClusterIP
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    baseLabels(cluster, role, region.Name),
		},
		Spec: corev1.ServiceSpec{
			Type:     typ,
			Selector: selectorLabels(cluster.Name, role, region.Name),
			Ports: []corev1.ServicePort{
				{Name: "rest", Port: 8080, TargetPort: intOrStr("rest")},
				{Name: "grpc", Port: 50051, TargetPort: intOrStr("grpc")},
				{Name: "pg", Port: 5432, TargetPort: intOrStr("pg")},
			},
		},
	}
}
