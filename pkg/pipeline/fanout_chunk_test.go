package pipeline_test

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/sink"
	"github.com/gribovan2005/drift/pkg/vector"
)

// TestBroadcast_ChunkFanOutIsolated proves a chunk-record fanned out to two branches
// is deep-copied per branch: one branch mutates its batch in place (MapInt64 ×10)
// while the other only reads it (Filter keep-all). With per-branch cloning the reading
// branch sees the original values; sharing one batch would corrupt it (and trip -race).
func TestBroadcast_ChunkFanOutIsolated(t *testing.T) {
	batch := vector.GenInt64("v", 1, 4, func(i int) int64 { return int64(i + 1) }) // [1,2,3,4]
	out := sink.NewMemory()

	// DAG: a (passthrough) → {b: ×10 in place, c: read-only}. Both feed the sink.
	p := pipeline.New(
		vector.MemSource(batch),
		[]pipeline.Stage{
			{Label: "a", Op: vector.FilterInt64("v", func(int64) bool { return true }), Next: []string{"b", "c"}},
			{Label: "b", Op: vector.MapInt64("v", func(x int64) int64 { return x * 10 })},
			{Label: "c", Op: vector.FilterInt64("v", func(int64) bool { return true })},
		},
		out,
	)
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	var sum, rows int64
	for _, r := range out.Records() {
		if r.Chunk == nil {
			continue
		}
		for _, v := range r.Chunk.Int64("v") {
			sum += v
			rows++
		}
	}
	// branch c: 1+2+3+4 = 10 ; branch b: 10+20+30+40 = 100 ; total 110 over 8 rows.
	if rows != 8 || sum != 110 {
		t.Fatalf("got rows=%d sum=%d, want rows=8 sum=110 (branches corrupted each other)", rows, sum)
	}
}

// TestBroadcast_LinearNoClone is a guard that the single-dst (linear) path still works
// and passes chunk records straight through (no clone needed, no corruption).
func TestBroadcast_LinearNoClone(t *testing.T) {
	batch := vector.GenInt64("v", 1, 3, func(i int) int64 { return int64(i) })
	out := sink.NewMemory()
	p := pipeline.New(
		vector.MemSource(batch),
		[]pipeline.Stage{{Label: "m", Op: vector.MapInt64("v", func(x int64) int64 { return x + 1 })}},
		out,
	)
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	var got []int64
	for _, r := range out.Records() {
		if r.Chunk != nil {
			got = append(got, r.Chunk.Int64("v")...)
		}
	}
	want := []int64{1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}
