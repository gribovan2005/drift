package bench

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/operator"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/sdk"
)

func humanRate(eps float64) string {
	switch {
	case eps >= 1e6:
		return fmt.Sprintf("%6.2f M/s", eps/1e6)
	case eps >= 1e3:
		return fmt.Sprintf("%6.1f k/s", eps/1e3)
	default:
		return fmt.Sprintf("%6.0f /s", eps)
	}
}

// TestBeastModeThroughput demonstrates what the Dedicated ("beast") profile buys
// over the defaults, honestly split into the two effects that actually exist:
//
//	Part 1 — a LINEAR pipeline: the win comes from the bigger batch/buffers
//	         (single-source ingestion is the ceiling, so cores do NOT help here).
//	Part 2 — a COMPUTE-BOUND stage wrapped in pipeline.Parallel: here owning the
//	         node and giving it cores scales throughput ~linearly.
//
// Run:  go test ./tests/bench/ -run BeastMode -v -count=1
func TestBeastModeThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("beast throughput skipped in -short")
	}
	if raceEnabled {
		t.Skip("beast throughput skipped under -race (instrumentation distorts timing)")
	}

	// ── Part 1: linear pipeline — batch/buffer effect, cores pinned to 1 ──────
	prevProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prevProcs)
	prevGC := debug.SetGCPercent(100)
	defer debug.SetGCPercent(prevGC)

	const nLight = 5_000_000
	light := makeBenchRecords(nLight)
	inc := func(r core.Record) (core.Record, error) {
		r.Payload["v"] = r.Payload["v"].(int) + 1
		return r, nil
	}
	runLinear := func(opts ...sdk.Option) float64 {
		start := time.Now()
		err := sdk.New(opts...).From(sdk.Slice(light)).Map(inc).To(sdk.Discard()).Run(context.Background())
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		return float64(nLight) / time.Since(start).Seconds()
	}

	base := runLinear()                                                 // batch 64, buffer 256
	beast := runLinear(sdk.WithProfile(sdk.Dedicated))                  // batch 512, buffer 1024
	debug.SetGCPercent(200)                                             // GC knob from the profile
	beastGC := runLinear(sdk.WithProfile(sdk.Dedicated))
	debug.SetGCPercent(100)

	t.Logf("── Part 1: LINEAR map, GOMAXPROCS=1, %d records ──", nLight)
	t.Logf("  default  (batch 64, GOGC 100):  %s", humanRate(base))
	t.Logf("  beast    (batch 512, GOGC 100): %s  (%.2fx)", humanRate(beast), beast/base)
	t.Logf("  beast+GC (batch 512, GOGC 200): %s  (%.2fx)", humanRate(beastGC), beastGC/base)

	// ── Part 2: compute-bound stage + Parallel — core scaling ────────────────
	const nHeavy = 2_000_000
	heavy := makeBenchRecords(nHeavy)
	heavyOp := func() core.Operator {
		return operator.NewMap(func(r core.Record) (core.Record, error) {
			x := r.Payload["v"].(int)
			for range 4000 { // genuinely CPU-bound: ~µs of real work per record
				x = (x*1664525 + 1013904223) & 0x7fffffff
			}
			r.Payload["v"] = x
			return r, nil
		})
	}
	runHeavy := func(procs, shards int) float64 {
		runtime.GOMAXPROCS(procs)
		ops := make([]core.Operator, shards)
		for i := range ops {
			ops[i] = heavyOp()
		}
		par := pipeline.Parallel(ops, nil) // round-robin; stateless op
		start := time.Now()
		err := sdk.New(sdk.WithProfile(sdk.Dedicated)).
			From(sdk.Slice(heavy)).Apply(par).To(sdk.Discard()).Run(context.Background())
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		return float64(nHeavy) / time.Since(start).Seconds()
	}

	t.Logf("── Part 2: COMPUTE-BOUND map + pipeline.Parallel, %d records ──", nHeavy)
	cpu := runtime.NumCPU()
	levels := []int{1, 2, 4}
	if cpu > 4 {
		levels = append(levels, cpu)
	}
	var oneCore float64
	for _, p := range levels {
		r := runHeavy(p, p)
		if p == 1 {
			oneCore = r
		}
		t.Logf("  GOMAXPROCS=%-2d shards=%-2d:  %s  (%.2fx vs 1 core)", p, p, humanRate(r), r/oneCore)
	}
}
