// Package vector is Drift's vectorized fast-lane: columnar operators that process
// core.Batch chunks instead of map[string]any rows, in tight per-column loops with
// no boxing and no per-row allocation. They implement core.Operator and travel the
// normal pipeline as "chunk-records" (core.Record.Chunk), so they compose with the
// SDK via Apply. See drift/Specs/Vectorized Fast-Lane.md.
//
// Scope: Int64/Float64 columns, Map/Filter. Other kinds, aggregations, windows and
// joins stay on the row (map[string]any) path.
package vector

import "github.com/gribovan2005/drift/pkg/core"

// MapInt64 applies fn to every value of the named int64 column, in place, for each
// chunk in the batch. Records without a Chunk (stray row records) pass through.
func MapInt64(field string, fn func(int64) int64) core.Operator {
	return &mapInt64{field: field, fn: fn}
}

type mapInt64 struct {
	field string
	fn    func(int64) int64
}

func (m *mapInt64) Process(in []core.Record) ([]core.Record, error) {
	for _, r := range in {
		if r.Chunk == nil {
			continue
		}
		col := r.Chunk.Int64(m.field)
		for i := range col {
			col[i] = m.fn(col[i])
		}
	}
	return in, nil
}
func (m *mapInt64) OnSchemaChange(core.Schema) {}

// MapFloat64 applies fn to every value of the named float64 column, in place.
func MapFloat64(field string, fn func(float64) float64) core.Operator {
	return &mapFloat64{field: field, fn: fn}
}

type mapFloat64 struct {
	field string
	fn    func(float64) float64
}

func (m *mapFloat64) Process(in []core.Record) ([]core.Record, error) {
	for _, r := range in {
		if r.Chunk == nil {
			continue
		}
		col := r.Chunk.Float64(m.field)
		for i := range col {
			col[i] = m.fn(col[i])
		}
	}
	return in, nil
}
func (m *mapFloat64) OnSchemaChange(core.Schema) {}

// FilterInt64 keeps only rows whose named int64 column satisfies pred, compacting
// all columns in place and updating Batch.Len.
func FilterInt64(field string, pred func(int64) bool) core.Operator {
	return &filterInt64{field: field, pred: pred}
}

type filterInt64 struct {
	field string
	pred  func(int64) bool
}

func (f *filterInt64) Process(in []core.Record) ([]core.Record, error) {
	for _, r := range in {
		b := r.Chunk
		if b == nil {
			continue
		}
		col := b.Int64(f.field)
		w := 0
		for i := 0; i < b.Len; i++ {
			if f.pred(col[i]) {
				if w != i {
					b.CopyRow(w, i)
				}
				w++
			}
		}
		b.Truncate(w)
	}
	return in, nil
}
func (f *filterInt64) OnSchemaChange(core.Schema) {}

// FilterFloat64 keeps only rows whose named float64 column satisfies pred.
func FilterFloat64(field string, pred func(float64) bool) core.Operator {
	return &filterFloat64{field: field, pred: pred}
}

type filterFloat64 struct {
	field string
	pred  func(float64) bool
}

func (f *filterFloat64) Process(in []core.Record) ([]core.Record, error) {
	for _, r := range in {
		b := r.Chunk
		if b == nil {
			continue
		}
		col := b.Float64(f.field)
		w := 0
		for i := 0; i < b.Len; i++ {
			if f.pred(col[i]) {
				if w != i {
					b.CopyRow(w, i)
				}
				w++
			}
		}
		b.Truncate(w)
	}
	return in, nil
}
func (f *filterFloat64) OnSchemaChange(core.Schema) {}
