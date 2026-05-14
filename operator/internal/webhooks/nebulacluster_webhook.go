// Package webhooks contains admission webhooks for the operator's CRDs.
//
// Today we ship a validating webhook for NebulaCluster only — the two most
// painful user errors (downgrade without snapshot, SwapRebalance with no
// backend) deserve hard rejection rather than event-log warnings.
package webhooks

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	nebulav1alpha1 "github.com/bwalia/nebuladb-operator/api/v1alpha1"
)

// +kubebuilder:webhook:path=/validate-nebula-nebuladb-io-v1alpha1-nebulacluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=nebula.nebuladb.io,resources=nebulaclusters,verbs=create;update,versions=v1alpha1,name=vnebulacluster.nebula.nebuladb.io,admissionReviewVersions=v1

// NebulaClusterValidator is a simple validating webhook.
type NebulaClusterValidator struct{}

var _ webhook.CustomValidator = &NebulaClusterValidator{}

func (v *NebulaClusterValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return v.validate(obj.(*nebulav1alpha1.NebulaCluster), nil)
}

func (v *NebulaClusterValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	return v.validate(newObj.(*nebulav1alpha1.NebulaCluster), oldObj.(*nebulav1alpha1.NebulaCluster))
}

func (v *NebulaClusterValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

func (v *NebulaClusterValidator) validate(c *nebulav1alpha1.NebulaCluster, old *nebulav1alpha1.NebulaCluster) (admission.Warnings, error) {
	var warnings admission.Warnings
	var errs field.ErrorList

	if c.Spec.Version == "" {
		errs = append(errs, field.Required(field.NewPath("spec", "version"), "must be set to a container tag"))
	}

	// Replication knobs.
	if c.Spec.Replication.Followers < 0 {
		errs = append(errs, field.Invalid(field.NewPath("spec", "replication", "followers"),
			c.Spec.Replication.Followers, "must be >= 0"))
	}
	if c.Spec.Replication.Followers > 0 && !c.Spec.Replication.Enabled {
		warnings = append(warnings,
			"replication.followers>0 with replication.enabled=false — followers will not be provisioned")
	}

	// Raft mode (RFC 0003 Option A, issue #32). The boot-time check in
	// nebula_raft::RaftConfig::from_env enforces the same invariants;
	// duplicating here lets us reject a misconfigured CR at admission
	// instead of crash-looping the StatefulSet on rollout. ADR §7
	// is the source of truth.
	if c.Spec.Raft.Enabled {
		if c.Spec.Raft.Replicas < 3 {
			errs = append(errs, field.Invalid(
				field.NewPath("spec", "raft", "replicas"),
				c.Spec.Raft.Replicas,
				"raft requires at least 3 voting peers (ADR §7)"))
		}
		if c.Spec.Raft.Replicas%2 == 0 {
			errs = append(errs, field.Invalid(
				field.NewPath("spec", "raft", "replicas"),
				c.Spec.Raft.Replicas,
				"raft replicas must be odd to form a clean quorum (ADR §7)"))
		}
		// Two write paths in one cluster is split-brain by construction.
		if c.Spec.Replication.Enabled {
			errs = append(errs, field.Forbidden(
				field.NewPath("spec", "raft", "enabled"),
				"raft.enabled and replication.enabled are mutually exclusive — pick one write-availability model"))
		}
		// Catch the common shrink mistake on update: dropping voters
		// below quorum without a graceful change_membership will stall
		// the cluster. Operators have to drive that explicitly via
		// /admin/raft/change-membership; the webhook can only warn.
		if old != nil && old.Spec.Raft.Enabled && c.Spec.Raft.Replicas < old.Spec.Raft.Replicas {
			warnings = append(warnings,
				fmt.Sprintf("raft.replicas shrinking %d -> %d; operator must drive this via /admin/raft/change-membership before scaling the StatefulSet, or quorum will stall",
					old.Spec.Raft.Replicas, c.Spec.Raft.Replicas))
		}
		// Toggling raft on/off after the fact is not supported in v1
		// (ADR §11: no online migration from standalone -> raft). Reject
		// hot-toggle so the failure mode is "admission rejection at
		// kubectl apply time" instead of "data loss on next pod restart."
		if old != nil && old.Spec.Raft.Enabled != c.Spec.Raft.Enabled {
			errs = append(errs, field.Forbidden(
				field.NewPath("spec", "raft", "enabled"),
				"raft.enabled is immutable after creation — wipe + re-create the cluster to switch modes (ADR §11, no online migration in v1)"))
		}
	} else if c.Spec.Raft.Replicas != 0 {
		// raft.replicas set without raft.enabled is almost always a typo
		// or a half-converted manifest — warn loudly. Not an error
		// because legacy manifests with the field defaulted via webhook
		// rewrite shouldn't trip rollback.
		warnings = append(warnings,
			"spec.raft.replicas is set but spec.raft.enabled=false; the field will be ignored")
	}

	// Autoscaling sanity checks.
	if c.Spec.Autoscaling.Enabled {
		if c.Spec.Autoscaling.MinFollowers > c.Spec.Autoscaling.MaxFollowers {
			errs = append(errs, field.Invalid(field.NewPath("spec", "autoscaling"),
				c.Spec.Autoscaling, "minFollowers must be <= maxFollowers"))
		}
	}

	// SwapRebalance is the common foot-gun — reject early.
	if c.Spec.UpgradeStrategy == nebulav1alpha1.UpgradeSwapRebalance {
		warnings = append(warnings,
			"upgradeStrategy=SwapRebalance is not implemented in NebulaDB today; operator will fall back to Recreate")
	}

	// Region names must be unique.
	seen := map[string]struct{}{}
	for i, region := range c.Spec.Regions {
		if region.Name == "" {
			errs = append(errs, field.Required(field.NewPath("spec", "regions").Index(i).Child("name"),
				"region name is required"))
			continue
		}
		if _, dup := seen[region.Name]; dup {
			errs = append(errs, field.Duplicate(field.NewPath("spec", "regions").Index(i).Child("name"), region.Name))
		}
		seen[region.Name] = struct{}{}
	}

	// AI embedding dim must match when using OpenAI/Ollama and a dim is set.
	if c.Spec.AI.Enabled && c.Spec.AI.EmbeddingDim < 0 {
		errs = append(errs, field.Invalid(field.NewPath("spec", "ai", "embeddingDim"),
			c.Spec.AI.EmbeddingDim, "must be >= 0"))
	}

	// Immutable-on-update checks.
	if old != nil {
		// Prevent silent storage shrink — the CSI driver may reject it and
		// the user ends up with a half-provisioned cluster.
		if old.Spec.Storage.Size != "" && c.Spec.Storage.Size != "" && old.Spec.Storage.Size != c.Spec.Storage.Size {
			warnings = append(warnings,
				fmt.Sprintf("storage.size changed %s -> %s; most CSI drivers do not allow shrink",
					old.Spec.Storage.Size, c.Spec.Storage.Size))
		}
		// Image repo changes should be rare and deliberate — warn loudly.
		if old.Spec.Image != "" && c.Spec.Image != "" && old.Spec.Image != c.Spec.Image {
			warnings = append(warnings,
				fmt.Sprintf("image changed %s -> %s; ensure new image is a compatible NebulaDB build",
					old.Spec.Image, c.Spec.Image))
		}
	}

	if len(errs) > 0 {
		gk := schema.GroupKind{Group: "nebula.nebuladb.io", Kind: "NebulaCluster"}
		return warnings, apierrors.NewInvalid(gk, c.Name, errs)
	}
	return warnings, nil
}

// SetupWithManager wires the validator into the manager.
func SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&nebulav1alpha1.NebulaCluster{}).
		WithValidator(&NebulaClusterValidator{}).
		Complete()
}
