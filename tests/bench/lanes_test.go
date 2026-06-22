package bench

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/vector"
)

// TestLanesThroughput measures N fully independent pipelines (no shared channel) on
// the same compute-bound columnar workload — to show they scale closer to linear
// than the shared-channel parallel lane (MaxLane, ~5.4x at 8).
//
//	go test ./tests/bench/ -run Lanes -v -count=1
func TestLanesThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("lanes bench skipped in -short")
	}
	if raceEnabled {
		t.Skip("lanes bench skipped under -race (instrumentation distorts timing)")
	}
	prev := runtime.GOMAXPROCS(runtime.NumCPU())
	defer runtime.GOMAXPROCS(prev)

	const n, chunk = 4_000_000, 4096
	heavy := func(x int64) int64 {
		for range 400 {
			x = (x*1664525 + 1013904223) & 0x7fffffff
		}
		return x
	}
	rate := func(eps float64) string { return fmt.Sprintf("%7.2f M/s", eps/1e6) }

	run := func(lanesN int) float64 {
		// Each lane gets its own batches (own source + sink, no shared channel).
		per := n / lanesN
		lanes := make([]*pipeline.Pipeline, lanesN)
		for l := range lanes {
			batches := vector.GenInt64("v", per/chunk, chunk, func(i int) int64 { return int64(i) })
			lanes[l] = pipeline.New(
				vector.MemSource(batches),
				[]pipeline.Stage{{Label: "map", Op: vector.MapInt64("v", heavy)}},
				vector.Discard(),
			)
		}
		start := time.Now()
		if err := pipeline.RunLanes(context.Background(), lanes...); err != nil {
			t.Fatalf("lanes=%d: %v", lanesN, err)
		}
		return float64((per/chunk)*chunk*lanesN) / time.Since(start).Seconds()
	}

	t.Logf("── Independent N-lane pipelines (compute-bound), GOMAXPROCS=%d, %d rows ──", runtime.NumCPU(), n)
	var one float64
	for _, l := range []int{1, 2, 4, 8} {
		r := run(l)
		if l == 1 {
			one = r
		}
		t.Logf("  %d lanes:  %s  (%.2fx vs 1)", l, rate(r), r/one)
	}
}
