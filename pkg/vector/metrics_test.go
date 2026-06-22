package vector_test

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/vector"
)

// TestChunkRowMetrics verifies pipeline metrics count logical rows, not chunk-
// records: 10 chunks × 100 rows must report ProcessedTotal == 1000, not 10.
func TestChunkRowMetrics(t *testing.T) {
	batches := vector.GenInt64("v", 10, 100, func(i int) int64 { return int64(i) })
	p := pipeline.New(
		vector.MemSource(batches),
		[]pipeline.Stage{{Label: "map", Op: vector.MapInt64("v", func(x int64) int64 { return x + 1 })}},
		vector.Discard(),
	)
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	snap := p.Snapshot()
	if len(snap.Stages) != 1 {
		t.Fatalf("stages = %d, want 1", len(snap.Stages))
	}
	if got := snap.Stages[0].ProcessedTotal; got != 1000 {
		t.Fatalf("ProcessedTotal = %d, want 1000 (rows, not 10 chunks)", got)
	}
}
