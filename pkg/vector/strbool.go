package vector

import "github.com/gribovan2005/drift/pkg/core"

// MapString applies fn to every value of the named string column, in place.
func MapString(field string, fn func(string) string) core.Operator {
	return &mapString{field: field, fn: fn}
}

type mapString struct {
	field string
	fn    func(string) string
}

func (m *mapString) Process(in []core.Record) ([]core.Record, error) {
	for _, r := range in {
		if r.Chunk == nil {
			continue
		}
		col := r.Chunk.String(m.field)
		for i := range col {
			col[i] = m.fn(col[i])
		}
	}
	return in, nil
}
func (m *mapString) OnSchemaChange(core.Schema) {}

// FilterString keeps only rows whose named string column satisfies pred,
// compacting all columns in place.
func FilterString(field string, pred func(string) bool) core.Operator {
	return &filterString{field: field, pred: pred}
}

type filterString struct {
	field string
	pred  func(string) bool
}

func (f *filterString) Process(in []core.Record) ([]core.Record, error) {
	for _, r := range in {
		b := r.Chunk
		if b == nil {
			continue
		}
		col := b.String(f.field)
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
func (f *filterString) OnSchemaChange(core.Schema) {}

// FilterBool keeps only rows whose named bool column satisfies pred (e.g.
// FilterBool("verified", func(b bool) bool { return b }) keeps verified rows).
func FilterBool(field string, pred func(bool) bool) core.Operator {
	return &filterBool{field: field, pred: pred}
}

type filterBool struct {
	field string
	pred  func(bool) bool
}

func (f *filterBool) Process(in []core.Record) ([]core.Record, error) {
	for _, r := range in {
		b := r.Chunk
		if b == nil {
			continue
		}
		col := b.Bool(f.field)
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
func (f *filterBool) OnSchemaChange(core.Schema) {}
