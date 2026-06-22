package vector

import (
	"context"

	"github.com/gribovan2005/drift/pkg/core"
)

// MemSource emits each batch as one chunk-record (Record.Chunk = batch). The
// fast-lane source for tests, benchmarks, and bounded columnar jobs.
func MemSource(batches []*core.Batch) core.Source {
	return &memSource{batches: batches}
}

type memSource struct{ batches []*core.Batch }

func (m *memSource) Read(ctx context.Context) (<-chan core.Record, error) {
	ch := make(chan core.Record, 8)
	go func() {
		defer close(ch)
		for _, b := range m.batches {
			select {
			case ch <- core.Record{Chunk: b}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

// GenInt64 builds nBatches batches of `rows` rows, each with a single int64 column
// named field, filled by fill(globalRowIndex). A convenience for benchmarks/tests.
func GenInt64(field string, nBatches, rows int, fill func(i int) int64) []*core.Batch {
	schema := core.Schema{Fields: []core.Field{{Name: field, Type: core.FieldTypeInt}}}
	out := make([]*core.Batch, nBatches)
	idx := 0
	for bi := range out {
		col := make([]int64, rows)
		for j := range col {
			col[j] = fill(idx)
			idx++
		}
		out[bi] = &core.Batch{
			Schema: schema,
			Len:    rows,
			Cols:   []core.Column{{Kind: core.KindInt64, I64: col}},
		}
	}
	return out
}
