package sdk

import (
	"runtime"
	"runtime/debug"
	"testing"
)

func TestProfile_RuntimeSettings(t *testing.T) {
	mp, gc, ml := Sidecar.runtimeSettings()
	if want := maxInt(1, runtime.NumCPU()/4); mp != want {
		t.Errorf("Sidecar GOMAXPROCS = %d, want %d", mp, want)
	}
	if gc != 50 {
		t.Errorf("Sidecar GCPercent = %d, want 50", gc)
	}
	if ml != 0 {
		t.Errorf("Sidecar MemLimit = %d, want 0 (presets never set it)", ml)
	}

	mp, gc, _ = Dedicated.runtimeSettings()
	if mp != runtime.NumCPU() {
		t.Errorf("Dedicated GOMAXPROCS = %d, want %d", mp, runtime.NumCPU())
	}
	if gc != 200 {
		t.Errorf("Dedicated GCPercent = %d, want 200", gc)
	}
}

func TestWithProfile_NoOwnsProcess_LeavesRuntime(t *testing.T) {
	before := runtime.GOMAXPROCS(0)
	// Default profile (no OwnsProcess) must not touch process-global runtime.
	_ = New(WithProfile(Sidecar))
	if after := runtime.GOMAXPROCS(0); after != before {
		t.Fatalf("GOMAXPROCS changed %d→%d without OwnsProcess()", before, after)
	}
}

func TestWithProfile_OwnsProcess_AppliesRuntime(t *testing.T) {
	// Not parallel: this mutates process-global state. Save & restore around it.
	prevProcs := runtime.GOMAXPROCS(0)
	prevGC := debug.SetGCPercent(100) // sets a known value, returns the prior one
	defer func() {
		runtime.GOMAXPROCS(prevProcs)
		debug.SetGCPercent(prevGC)
	}()

	_ = New(WithProfile(Dedicated.OwnsProcess()))

	if got := runtime.GOMAXPROCS(0); got != runtime.NumCPU() {
		t.Errorf("GOMAXPROCS = %d, want %d", got, runtime.NumCPU())
	}
	// Reading GC percent: SetGCPercent returns the current value (and sets it back).
	applied := debug.SetGCPercent(200)
	if applied != 200 {
		t.Errorf("GCPercent applied = %d, want 200", applied)
	}
}
