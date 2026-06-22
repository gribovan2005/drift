package bench

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/vector"
	"github.com/gribovan2005/drift/sdk"
)

// TestVectorParallelStage shows that a CPU-heavy vectorized stage scales across
// cores when wrapped in vector.Parallel — breaking the single-stage ceiling. A
// trivial map is source-bound, so the per-row work here is deliberately heavy
// (~µs) to expose stage parallelism.
//
// Run:  go test ./tests/bench/ -run VectorParallelStage -v -count=1
func TestVectorParallelStage(t *testing.T) {
	if testing.Short() {
		t.Skip("vector-parallel bench skipped in -short")
	}
	if raceEnabled {
		t.Skip("vector-parallel bench skipped under -race (instrumentation distorts timing)")
	}
	prev := runtime.GOMAXPROCS(runtime.NumCPU())
	defer runtime.GOMAXPROCS(prev)

	const rows, chunk = 2_000_000, 4096
	heavy := func(x int64) int64 {
		for range 2000 { // ~µs of CPU per row
			x = (x*1664525 + 1013904223) & 0x7fffffff
		}
		return x
	}
	rate := func(eps float64) string { return fmt.Sprintf("%7.2f M/s", eps/1e6) }

	runMap := func(op core.Operator) float64 {
		batches := vector.GenInt64("v", rows/chunk, chunk, func(i int) int64 { return int64(i) })
		start := time.Now()
		err := sdk.New().From(vector.MemSource(batches)).Apply(op).To(vector.Discard()).Run(context.Background())
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		return float64((rows/chunk)*chunk) / time.Since(start).Seconds()
	}

	t.Logf("── Heavy vectorized map, GOMAXPROCS=%d, %d rows ──", runtime.NumCPU(), rows)
	one := runMap(vector.MapInt64("v", heavy))
	t.Logf("  1 shard  (single stage):  %s  (1.00x)", rate(one))
	for _, n := range []int{2, 4, 8} {
		r := runMap(vector.Parallel(n, func() core.Operator { return vector.MapInt64("v", heavy) }))
		t.Logf("  %d shards (Parallel):      %s  (%.2fx)", n, rate(r), r/one)
	}
}
