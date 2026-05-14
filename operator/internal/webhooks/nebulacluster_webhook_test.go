package webhooks

import (
	"context"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	nebulav1alpha1 "github.com/bwalia/nebuladb-operator/api/v1alpha1"
)

// validClusterBase is a minimal CR that passes every existing
// validation rule. Tests narrow it to exercise one rule at a time so a
// failure points unambiguously at the field under test.
func validClusterBase() *nebulav1alpha1.NebulaCluster {
	return &nebulav1alpha1.NebulaCluster{
		Spec: nebulav1alpha1.NebulaClusterSpec{
			Version: "0.1.0",
		},
	}
}

// statusErrFields extracts the field paths from an Invalid/Forbidden
// errror returned by the webhook so tests can assert on them without
// caring about message wording.
func statusErrFields(t *testing.T, err error) []string {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	se, ok := err.(*apierrors.StatusError)
	if !ok {
		t.Fatalf("expected StatusError, got %T: %v", err, err)
	}
	if se.ErrStatus.Details == nil {
		t.Fatalf("missing details on StatusError: %v", err)
	}
	out := make([]string, 0, len(se.ErrStatus.Details.Causes))
	for _, c := range se.ErrStatus.Details.Causes {
		out = append(out, c.Field)
	}
	return out
}

// Phase 2.5h — raft webhook validation.

func TestRaft_AcceptsThreePeers(t *testing.T) {
	v := &NebulaClusterValidator{}
	c := validClusterBase()
	c.Spec.Raft.Enabled = true
	c.Spec.Raft.Replicas = 3
	if _, err := v.ValidateCreate(context.Background(), c); err != nil {
		t.Fatalf("3 peers should pass: %v", err)
	}
}

func TestRaft_AcceptsFivePeers(t *testing.T) {
	v := &NebulaClusterValidator{}
	c := validClusterBase()
	c.Spec.Raft.Enabled = true
	c.Spec.Raft.Replicas = 5
	if _, err := v.ValidateCreate(context.Background(), c); err != nil {
		t.Fatalf("5 peers should pass: %v", err)
	}
}

func TestRaft_RejectsTwoPeers(t *testing.T) {
	v := &NebulaClusterValidator{}
	c := validClusterBase()
	c.Spec.Raft.Enabled = true
	c.Spec.Raft.Replicas = 2
	_, err := v.ValidateCreate(context.Background(), c)
	fields := statusErrFields(t, err)
	// 2 trips both "min 3" and "must be odd" — that's actually fine,
	// the user gets two clear pointers. Just assert at least one of
	// them flagged the right field.
	hit := false
	for _, f := range fields {
		if f == "spec.raft.replicas" {
			hit = true
			break
		}
	}
	if !hit {
		t.Fatalf("expected spec.raft.replicas in error fields, got %v", fields)
	}
}

func TestRaft_RejectsZeroPeersWhenEnabled(t *testing.T) {
	v := &NebulaClusterValidator{}
	c := validClusterBase()
	c.Spec.Raft.Enabled = true
	// Replicas defaults to 0 which is < 3.
	_, err := v.ValidateCreate(context.Background(), c)
	if err == nil {
		t.Fatalf("0 replicas with enabled should fail")
	}
}

func TestRaft_RejectsEvenPeers(t *testing.T) {
	v := &NebulaClusterValidator{}
	c := validClusterBase()
	c.Spec.Raft.Enabled = true
	c.Spec.Raft.Replicas = 4
	_, err := v.ValidateCreate(context.Background(), c)
	if err == nil {
		t.Fatalf("4 peers should fail (even)")
	}
	if !strings.Contains(err.Error(), "odd") {
		t.Fatalf("expected 'odd' in error message: %v", err)
	}
}

func TestRaft_RejectsCoexistenceWithReplication(t *testing.T) {
	v := &NebulaClusterValidator{}
	c := validClusterBase()
	c.Spec.Raft.Enabled = true
	c.Spec.Raft.Replicas = 3
	c.Spec.Replication.Enabled = true
	_, err := v.ValidateCreate(context.Background(), c)
	if err == nil {
		t.Fatalf("raft.enabled + replication.enabled should fail")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected 'mutually exclusive' in error: %v", err)
	}
}

func TestRaft_DisabledIgnoresReplicas(t *testing.T) {
	// raft.enabled=false with replicas=4 (even) should still pass —
	// the field is ignored, only a warning is emitted.
	v := &NebulaClusterValidator{}
	c := validClusterBase()
	c.Spec.Raft.Enabled = false
	c.Spec.Raft.Replicas = 4 // would be invalid if enabled
	warnings, err := v.ValidateCreate(context.Background(), c)
	if err != nil {
		t.Fatalf("disabled raft should not error: %v", err)
	}
	// Confirm the user gets a warning so they know the field is dead.
	hit := false
	for _, w := range warnings {
		if strings.Contains(w, "raft.replicas") {
			hit = true
			break
		}
	}
	if !hit {
		t.Fatalf("expected warning about ignored raft.replicas, got %v", warnings)
	}
}

func TestRaft_RejectsHotToggle(t *testing.T) {
	// raft.enabled is immutable after creation. ADR §11 says no
	// online migration from standalone to raft (or vice versa);
	// admission rejection > data loss.
	v := &NebulaClusterValidator{}
	old := validClusterBase()
	old.Spec.Raft.Enabled = false

	newCR := validClusterBase()
	newCR.Spec.Raft.Enabled = true
	newCR.Spec.Raft.Replicas = 3

	_, err := v.ValidateUpdate(context.Background(), old, newCR)
	if err == nil {
		t.Fatalf("toggling raft.enabled on update should fail")
	}
	if !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("expected 'immutable' in error: %v", err)
	}
}

func TestRaft_WarnsOnShrinkBelowOldReplicas(t *testing.T) {
	v := &NebulaClusterValidator{}
	old := validClusterBase()
	old.Spec.Raft.Enabled = true
	old.Spec.Raft.Replicas = 5

	newCR := validClusterBase()
	newCR.Spec.Raft.Enabled = true
	newCR.Spec.Raft.Replicas = 3 // valid count, but a shrink

	warnings, err := v.ValidateUpdate(context.Background(), old, newCR)
	if err != nil {
		t.Fatalf("shrinking 5->3 should warn, not error: %v", err)
	}
	hit := false
	for _, w := range warnings {
		if strings.Contains(w, "shrinking") || strings.Contains(w, "change-membership") {
			hit = true
			break
		}
	}
	if !hit {
		t.Fatalf("expected shrink warning, got %v", warnings)
	}
}
