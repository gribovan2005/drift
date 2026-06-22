package job

import (
	"strings"
	"testing"
)

const profileJobYAML = `
name: prof
profile: %s
source:
  type: memory
stages:
  - label: tag
    op: map-set
    field: seen
    value: true
sink:
  type: memory
`

func TestLoad_ValidProfile(t *testing.T) {
	built, err := Load([]byte(strings.Replace(profileJobYAML, "%s", "sidecar", 1)))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if built.Spec.Profile != "sidecar" {
		t.Fatalf("profile = %q, want sidecar", built.Spec.Profile)
	}
	// Built.Pipeline must apply the profile without panicking and yield a pipeline.
	if p := built.Pipeline(); p == nil {
		t.Fatal("Pipeline() returned nil")
	}
}

func TestLoad_UnknownProfile(t *testing.T) {
	_, err := Load([]byte(strings.Replace(profileJobYAML, "%s", "turbo", 1)))
	if err == nil {
		t.Fatal("expected error for unknown profile")
	}
	if !strings.Contains(err.Error(), "unknown profile") {
		t.Fatalf("error = %v, want 'unknown profile'", err)
	}
}

func TestLoad_NoProfile(t *testing.T) {
	// profile is optional — a spec without it loads fine.
	y := strings.Replace(strings.Replace(profileJobYAML, "profile: %s\n", "", 1), "%s", "", 1)
	built, err := Load([]byte(y))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if built.Spec.Profile != "" {
		t.Fatalf("profile = %q, want empty", built.Spec.Profile)
	}
}
