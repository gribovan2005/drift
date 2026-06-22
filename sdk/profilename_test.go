package sdk

import "testing"

func TestProfileByName(t *testing.T) {
	if p, ok := ProfileByName("sidecar"); !ok || p.BatchSize != Sidecar.BatchSize {
		t.Fatalf("sidecar lookup failed: %+v ok=%v", p, ok)
	}
	if p, ok := ProfileByName("DEDICATED"); !ok || p.BatchSize != Dedicated.BatchSize {
		t.Fatalf("dedicated lookup (case-insensitive) failed: %+v ok=%v", p, ok)
	}
	if _, ok := ProfileByName("nope"); ok {
		t.Fatal("unknown profile should return ok=false")
	}
}

func TestProfile_PipelineOptions(t *testing.T) {
	// Local options only (3: batch, buffer, linger) — no process-global side effects.
	if n := len(Sidecar.PipelineOptions()); n != 3 {
		t.Fatalf("PipelineOptions len = %d, want 3", n)
	}
}
