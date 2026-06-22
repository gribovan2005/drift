package bench

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/vector"
)

// TestDistGroupByThroughput measures a distributed columnar GROUP BY: N independent
// lanes each compute a partial group-by over their shard (no shared channel), then a
// single cheap MergeOp folds the per-lane partials into the global result. Because the
// merge cost scales with #keys (not #rows), throughput scales with lanes just like the
// raw N-lane bench — while staying global-correct for arbitrarily distributed input.
//
//	go test ./tests/bench/ -run DistGroupBy -v -count=1
func TestDistGroupByThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("dist group-by bench skipped in -short")
	}
	if raceEnabled {
		t.Skip("dist group-by bench skipped under -race (instrumentation distorts timing)")
	}
	prev := runtime.GOMAXPROCS(runtime.NumCPU())
	defer runtime.GOMAXPROCS(prev)

	const n, chunk, keys = 8_000_000, 4096, 2000
	rate := func(eps float64) string { return fmt.Sprintf("%7.2f M/s", eps/1e6) }

	// One shard's worth of int64-keyed batches (key = rowIdx % keys, value = qty).
	makeShard := func(rows int) []*core.Batch {
		nB := rows / chunk
		out := make([]*core.Batch, nB)
		idx := 0
		for b := range out {
			ks := make([]int64, chunk)
			qs := make([]int64, chunk)
			for j := range ks {
				ks[j] = int64(idx % keys)
				qs[j] = int64(idx % 100)
				idx++
			}
			out[b] = &core.Batch{
				Schema: core.Schema{Fields: []core.Field{
					{Name: "k", Type: core.FieldTypeInt},
					{Name: "qty", Type: core.FieldTypeInt},
				}},
				Len:  chunk,
				Cols: []core.Column{{Kind: core.KindInt64, I64: ks}, {Kind: core.KindInt64, I64: qs}},
			}
		}
		return out
	}
	gb := func() *vector.Group {
		return vector.GroupBy("k").Count("n").SumInt64("qty", "s").MaxInt64("qty", "mx")
	}

	run := func(lanesN int) float64 {
		per := n / lanesN
		collectors := make([]*vector.Collector, lanesN)
		lanes := make([]*pipeline.Pipeline, lanesN)
		for l := range lanes {
			collectors[l] = vector.Collect()
			lanes[l] = pipeline.New(
				vector.MemSource(makeShard(per)),
				[]pipeline.Stage{{Label: "partial", Op: gb().Op()}},
				collectors[l],
			)
		}
		start := time.Now()
		if err := pipeline.RunLanes(context.Background(), lanes...); err != nil {
			t.Fatalf("lanes=%d: %v", lanesN, err)
		}
		// Merge the per-lane partials into the global result (cheap: #keys rows).
		var partials []*core.Batch
		for _, c := range collectors {
			partials = append(partials, c.Batches()...)
		}
		merged := vector.Collect()
		mp := pipeline.New(vector.MemSource(partials),
			[]pipeline.Stage{{Label: "merge", Op: gb().MergeOp()}}, merged)
		if err := mp.Run(context.Background()); err != nil {
			t.Fatalf("merge: %v", err)
		}
		if got := merged.Batches()[0].Len; got != keys {
			t.Fatalf("lanes=%d: merged %d keys, want %d", lanesN, got, keys)
		}
		return float64((per/chunk)*chunk*lanesN) / time.Since(start).Seconds()
	}

	t.Logf("── Distributed GROUP BY (partial per lane + merge), GOMAXPROCS=%d, %d rows, %d keys ──",
		runtime.NumCPU(), n, keys)
	var one float64
	for _, l := range []int{1, 2, 4, 8} {
		r := run(l)
		if l == 1 {
			one = r
		}
		t.Logf("  %d lanes:  %s  (%.2fx vs 1)", l, rate(r), r/one)
	}
}
