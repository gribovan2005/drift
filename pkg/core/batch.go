package core

// ColumnKind identifies the typed storage a Column uses.
type ColumnKind uint8

const (
	KindInt64 ColumnKind = iota
	KindFloat64
	KindString
	KindBool
)

// Column is one typed column of a Batch. Exactly one of the typed slices is used,
// per Kind. Values are stored unboxed — no map, no interface{} — which is what
// makes the vectorized fast-lane allocation- and GC-cheap. See pkg/vector and
// drift/Specs/Vectorized Fast-Lane.md.
type Column struct {
	Kind ColumnKind
	I64  []int64
	F64  []float64
	Str  []string
	B    []bool

	// Null is an optional per-row validity mask: Null[i]==true marks cell i as NULL
	// (its typed slot holds a zero value to be ignored). A nil Null means the column
	// has no nulls — the common case, so existing all-valid columns stay zero-cost and
	// every operator that doesn't opt into nulls is unaffected. Produced e.g. by a
	// left-outer join for unmatched rows.
	Null []bool
}

// Batch is a columnar block of Len rows. Cols is parallel to Schema.Fields. It
// travels through the pipeline inside a Record (Record.Chunk) as a "chunk-record",
// so vectorized operators are ordinary core.Operators and the executor is
// unchanged.
type Batch struct {
	Schema Schema
	Len    int
	Cols   []Column
}

// Int64 returns the named int64 column, or nil if it is missing or not int64.
func (b *Batch) Int64(field string) []int64 {
	i := b.Schema.FieldIndex(field)
	if i < 0 || i >= len(b.Cols) || b.Cols[i].Kind != KindInt64 {
		return nil
	}
	return b.Cols[i].I64[:b.Len]
}

// Float64 returns the named float64 column, or nil if missing or not float64.
func (b *Batch) Float64(field string) []float64 {
	i := b.Schema.FieldIndex(field)
	if i < 0 || i >= len(b.Cols) || b.Cols[i].Kind != KindFloat64 {
		return nil
	}
	return b.Cols[i].F64[:b.Len]
}

// String returns the named string column, or nil if missing or not string.
func (b *Batch) String(field string) []string {
	i := b.Schema.FieldIndex(field)
	if i < 0 || i >= len(b.Cols) || b.Cols[i].Kind != KindString {
		return nil
	}
	return b.Cols[i].Str[:b.Len]
}

// Bool returns the named bool column, or nil if missing or not bool.
func (b *Batch) Bool(field string) []bool {
	i := b.Schema.FieldIndex(field)
	if i < 0 || i >= len(b.Cols) || b.Cols[i].Kind != KindBool {
		return nil
	}
	return b.Cols[i].B[:b.Len]
}

// IsNull returns the named column's null mask (truncated to Len), or nil if the
// column is missing or has no nulls. mask[i]==true means row i is NULL.
func (b *Batch) IsNull(field string) []bool {
	i := b.Schema.FieldIndex(field)
	if i < 0 || i >= len(b.Cols) || b.Cols[i].Null == nil {
		return nil
	}
	return b.Cols[i].Null[:b.Len]
}

// CopyRow copies row src to row dst across every column, keeping columns aligned.
// Used by vectorized Filter to compact in place. The null mask travels with the row.
func (b *Batch) CopyRow(dst, src int) {
	for ci := range b.Cols {
		c := &b.Cols[ci]
		switch c.Kind {
		case KindInt64:
			c.I64[dst] = c.I64[src]
		case KindFloat64:
			c.F64[dst] = c.F64[src]
		case KindString:
			c.Str[dst] = c.Str[src]
		case KindBool:
			c.B[dst] = c.B[src]
		}
		if c.Null != nil {
			c.Null[dst] = c.Null[src]
		}
	}
}

// Clone returns a deep copy of the batch: a fresh Schema.Fields slice and freshly
// allocated copies of every column's typed slices (and null mask). Vectorized
// operators mutate batches in place (Filter compacts, Map rewrites a column, join
// appends columns/fields), which is safe only while a chunk has a single owner — so
// the DAG executor clones a chunk before fanning it out to multiple branches. Returns
// nil for a nil receiver.
func (b *Batch) Clone() *Batch {
	if b == nil {
		return nil
	}
	nb := &Batch{Schema: b.Schema, Len: b.Len}
	nb.Schema.Fields = append([]Field(nil), b.Schema.Fields...)
	nb.Cols = make([]Column, len(b.Cols))
	for i, c := range b.Cols {
		nc := Column{Kind: c.Kind}
		if c.I64 != nil {
			nc.I64 = append([]int64(nil), c.I64...)
		}
		if c.F64 != nil {
			nc.F64 = append([]float64(nil), c.F64...)
		}
		if c.Str != nil {
			nc.Str = append([]string(nil), c.Str...)
		}
		if c.B != nil {
			nc.B = append([]bool(nil), c.B...)
		}
		if c.Null != nil {
			nc.Null = append([]bool(nil), c.Null...)
		}
		nb.Cols[i] = nc
	}
	return nb
}

// Truncate shrinks every column slice to n and sets Len = n.
func (b *Batch) Truncate(n int) {
	for ci := range b.Cols {
		c := &b.Cols[ci]
		switch c.Kind {
		case KindInt64:
			c.I64 = c.I64[:n]
		case KindFloat64:
			c.F64 = c.F64[:n]
		case KindString:
			c.Str = c.Str[:n]
		case KindBool:
			c.B = c.B[:n]
		}
		if c.Null != nil {
			c.Null = c.Null[:n]
		}
	}
	b.Len = n
}
