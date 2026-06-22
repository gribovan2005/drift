package vector

import (
	"sort"

	"github.com/gribovan2005/drift/pkg/core"
)

// FromRows bridges the row engine into the fast lane: it accumulates incoming row
// Records and emits columnar chunk-records of up to `size` rows each (Flush emits the
// partial final chunk). The columnar schema is inferred from the FIRST row — field
// names sorted for a stable column order, Go types mapped int/int64→Int64,
// float64→Float64, string→String, bool→Bool. A later row missing a field, or holding a
// value of a different type, yields a NULL cell for that column (via the validity
// mask). This is the row→columnar counterpart of ToRows; together they let a
// declarative (YAML/SDK) pipeline drop into the fast lane and back.
//
// `size` ≤ 0 defaults to 1024. Rows without a Payload (e.g. stray chunk-records) pass
// through untouched.
func FromRows(size int) core.Operator {
	if size <= 0 {
		size = 1024
	}
	return &rowBatcher{size: size}
}

type rowBatcher struct {
	size     int
	inferred bool
	fields   []core.Field
	cols     []core.Column // accumulation buffers (typed slices grow to n)
	n        int
}

func (o *rowBatcher) OnSchemaChange(core.Schema) {}

func (o *rowBatcher) Process(in []core.Record) ([]core.Record, error) {
	var out []core.Record
	for _, r := range in {
		if r.Payload == nil {
			out = append(out, r) // not a row record — pass through
			continue
		}
		if !o.inferred {
			o.infer(r.Payload)
		}
		o.appendRow(r.Payload)
		if o.n >= o.size {
			out = append(out, o.emit())
		}
	}
	return out, nil
}

// Flush emits the accumulated partial chunk (if any).
func (o *rowBatcher) Flush() ([]core.Record, error) {
	if o.n == 0 {
		return nil, nil
	}
	return []core.Record{o.emit()}, nil
}

// infer derives the columnar schema from the first row: sorted field names, kind from
// each value's Go type (unknown types default to String).
func (o *rowBatcher) infer(p map[string]any) {
	names := make([]string, 0, len(p))
	for k := range p {
		names = append(names, k)
	}
	sort.Strings(names)
	o.fields = make([]core.Field, len(names))
	for i, name := range names {
		kind := kindOf(p[name])
		o.fields[i] = core.Field{Name: name, Type: fieldTypeFor(kind)}
	}
	o.inferred = true
	o.resetCols()
}

func (o *rowBatcher) resetCols() {
	o.cols = make([]core.Column, len(o.fields))
	for i, f := range o.fields {
		o.cols[i] = core.Column{Kind: kindFor(f.Type)}
	}
	o.n = 0
}

// appendRow appends one row's values to the column buffers; missing or type-mismatched
// values become NULL cells.
func (o *rowBatcher) appendRow(p map[string]any) {
	for ci := range o.cols {
		c := &o.cols[ci]
		v, ok := p[o.fields[ci].Name]
		null := !ok
		switch c.Kind {
		case core.KindInt64:
			n, k := toInt64(v)
			if !k {
				null = true
			}
			c.I64 = append(c.I64, n)
		case core.KindFloat64:
			f, k := v.(float64)
			if !k {
				null = true
				f = 0
			}
			c.F64 = append(c.F64, f)
		case core.KindBool:
			b, k := v.(bool)
			if !k {
				null = true
				b = false
			}
			c.B = append(c.B, b)
		default: // String
			s, k := v.(string)
			if !k {
				null = true
				s = ""
			}
			c.Str = append(c.Str, s)
		}
		if null {
			// Backfill a nil mask to the current length, then mark this cell.
			if c.Null == nil {
				c.Null = make([]bool, o.n, o.size)
			}
			c.Null = append(c.Null, true)
		} else if c.Null != nil {
			c.Null = append(c.Null, false)
		}
	}
	o.n++
}

// emit builds a chunk from the accumulated columns and resets the buffers.
func (o *rowBatcher) emit() core.Record {
	b := &core.Batch{Schema: core.Schema{Fields: o.fields}, Len: o.n, Cols: o.cols}
	o.resetCols()
	return core.Record{Chunk: b}
}

func kindOf(v any) core.ColumnKind {
	switch v.(type) {
	case int, int64:
		return core.KindInt64
	case float64:
		return core.KindFloat64
	case bool:
		return core.KindBool
	default:
		return core.KindString
	}
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}

func fieldTypeFor(k core.ColumnKind) core.FieldType {
	switch k {
	case core.KindFloat64:
		return core.FieldTypeFloat
	case core.KindBool:
		return core.FieldTypeBool
	case core.KindString:
		return core.FieldTypeString
	default:
		return core.FieldTypeInt
	}
}

func kindFor(t core.FieldType) core.ColumnKind {
	switch t {
	case core.FieldTypeFloat:
		return core.KindFloat64
	case core.FieldTypeBool:
		return core.KindBool
	case core.FieldTypeString:
		return core.KindString
	default:
		return core.KindInt64
	}
}
