package bench

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/operator"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/sink"
	"github.com/gribovan2005/drift/pkg/source"
)

func makeBenchRecords(n int) []core.Record {
	recs := make([]core.Record, n)
	for i := range recs {
		recs[i] = core.Record{Payload: map[string]any{"v": i}}
	}
	return recs
}

// BenchmarkPipeline_MapFilter measures end-to-end throughput of a
// filter+map chain at various record counts.
func BenchmarkPipeline_MapFilter(b *testing.B) {
	const recordCount = 100_000
	records := makeBenchRecords(recordCount)

	stages := []pipeline.Stage{
		{
			Label: "filter",
			Op: operator.NewFilter(func(r core.Record) bool {
				return r.Payload["v"].(int)%2 == 0
			}),
		},
		{
			Label: "map",
			Op: operator.NewMap(func(r core.Record) (core.Record, error) {
				r.Payload["v"] = r.Payload["v"].(int) + 1
				return r, nil
			}),
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		src := source.NewMemory(records)
		snk := sink.NewMemory()
		p := pipeline.New(src, stages, snk)
		if err := p.Run(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(recordCount))
	}
}

// BenchmarkMap_ProcessBatch measures the raw cost of a single Map.Process call.
func BenchmarkMap_ProcessBatch(b *testing.B) {
	op := operator.NewMap(func(r core.Record) (core.Record, error) {
		r.Payload["v"] = r.Payload["v"].(int) * 2
		return r, nil
	})
	batch := makeBenchRecords(1000)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		if _, err := op.Process(batch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFilter_ProcessBatch measures the raw cost of a single Filter.Process call.
func BenchmarkFilter_ProcessBatch(b *testing.B) {
	op := operator.NewFilter(func(r core.Record) bool {
		return r.Payload["v"].(int)%2 == 0
	})
	batch := makeBenchRecords(1000)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		if _, err := op.Process(batch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDedup_AllUnique measures Deduplicate overhead when no records are
// dropped (best case — only the seen-map insert path).
func BenchmarkDedup_AllUnique(b *testing.B) {
	op := operator.NewDeduplicate(func(r core.Record) string {
		return fmt.Sprintf("%d", r.Payload["v"].(int))
	}, time.Hour)
	batch := makeBenchRecords(1000)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		// Reset seen map to avoid filling it across iterations.
		op = operator.NewDeduplicate(func(r core.Record) string {
			return fmt.Sprintf("%d", r.Payload["v"].(int))
		}, time.Hour)
		if _, err := op.Process(batch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDedup_AllDuplicates measures Deduplicate overhead when all records
// are dropped after the first (worst case for drops — measures map lookup path).
func BenchmarkDedup_AllDuplicates(b *testing.B) {
	op := operator.NewDeduplicate(func(r core.Record) string { return "x" }, time.Hour)
	batch := makeBenchRecords(1000)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		if _, err := op.Process(batch); err != nil {
			b.Fatal(err)
		}
	}
}
