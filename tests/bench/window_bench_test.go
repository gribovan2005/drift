package bench

import (
	"context"
	"testing"

	"github.com/andrejgribov/drift/pkg/core"
	"github.com/andrejgribov/drift/pkg/operator"
	"github.com/andrejgribov/drift/pkg/pipeline"
	"github.com/andrejgribov/drift/pkg/sink"
	"github.com/andrejgribov/drift/pkg/source"
)

func countAgg(window []core.Record) (core.Record, error) {
	return core.Record{Payload: map[string]any{"count": len(window)}}, nil
}

func BenchmarkTumblingWindow_Pipeline(b *testing.B) {
	const recordCount = 100_000
	records := makeBenchRecords(recordCount)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		src := source.NewMemory(records)
		snk := sink.NewMemory()
		w, _ := operator.NewTumblingWindow(64, countAgg)
		p := pipeline.New(src, []pipeline.Stage{{Label: "tumble", Op: w}}, snk)
		if err := p.Run(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(recordCount))
	}
}

func BenchmarkSlidingWindow_Pipeline(b *testing.B) {
	const recordCount = 100_000
	records := makeBenchRecords(recordCount)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		src := source.NewMemory(records)
		snk := sink.NewMemory()
		w, _ := operator.NewSlidingWindow(128, 32, countAgg)
		p := pipeline.New(src, []pipeline.Stage{{Label: "slide", Op: w}}, snk)
		if err := p.Run(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(recordCount))
	}
}

func BenchmarkTumblingWindow_ProcessBatch(b *testing.B) {
	w, _ := operator.NewTumblingWindow(64, countAgg)
	batch := makeBenchRecords(1000)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		if _, err := w.Process(batch); err != nil {
			b.Fatal(err)
		}
	}
}
