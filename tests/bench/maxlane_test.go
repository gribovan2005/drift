package bench

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/sink"
	"github.com/gribovan2005/drift/pkg/source"
	"github.com/gribovan2005/drift/pkg/vector"
	"github.com/gribovan2005/drift/sdk"
)

// TestMaxLaneThroughput measures the full parallel columnar lane — parallel decode
// (source.NewParallel of BinSource) → parallel vectorized compute (vector.Parallel)
// → parallel sink (sink.Parallel) — at increasing shard counts, to show the
// single-node end-to-end ceiling rising once every serial point is parallelised.
//
//	go test ./tests/bench/ -run MaxLane -v -count=1
func TestMaxLaneThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("max-lane bench skipped in -short")
	}
	if raceEnabled {
		t.Skip("max-lane bench skipped under -race (instrumentation distorts timing)")
	}
	prev := runtime.GOMAXPROCS(runtime.NumCPU())
	defer runtime.GOMAXPROCS(prev)

	const n, chunk = 4_000_000, 4096
	nB := n / chunk

	// Pre-encode binary columnar frames (produce-side, not timed).
	batches := vector.GenInt64("v", nB, chunk, func(i int) int64 { return int64(i) })
	frames := make([][]byte, nB)
	for i, b := range batches {
		f, err := vector.EncodeBatch(b)
		if err != nil {
			t.Fatal(err)
		}
		frames[i] = f
	}
	heavy := func(x int64) int64 {
		for range 400 { // ~µs of CPU per row, so compute scales with shards
			x = (x*1664525 + 1013904223) & 0x7fffffff
		}
		return x
	}
	rate := func(eps float64) string { return fmt.Sprintf("%7.2f M/s", eps/1e6) }

	run := func(shards int) float64 {
		subs := make([]core.Source, shards)
		for s := range subs {
			subs[s] = vector.BinSource(shardOf(frames, s, shards)) // decode ∥
		}
		start := time.Now()
		err := sdk.New().
			From(source.NewParallel(subs...)).
			Apply(vector.Parallel(shards, func() core.Operator { return vector.MapInt64("v", heavy) })). // compute ∥
			To(sink.Parallel(shards, func() core.Sink { return vector.Discard() })).                    // sink ∥
			Run(context.Background())
		if err != nil {
			t.Fatalf("shards=%d: %v", shards, err)
		}
		return float64(nB*chunk) / time.Since(start).Seconds()
	}

	t.Logf("── Full parallel columnar lane (decode∥ + compute∥ + sink∥), GOMAXPROCS=%d, %d rows ──", runtime.NumCPU(), n)
	var one float64
	for _, s := range []int{1, 2, 4, 8} {
		r := run(s)
		if s == 1 {
			one = r
		}
		t.Logf("  shards=%-2d:  %s  (%.2fx vs 1)", s, rate(r), r/one)
	}
}

// shardOf returns every s-th frame starting at offset (round-robin shard).
func shardOf(frames [][]byte, offset, stride int) [][]byte {
	var out [][]byte
	for i := offset; i < len(frames); i += stride {
		out = append(out, frames[i])
	}
	return out
}
