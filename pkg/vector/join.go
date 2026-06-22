package vector

import (
	"fmt"

	"github.com/gribovan2005/drift/pkg/core"
)

// HashJoin builds a vectorized build-side hash join: a lookup table is built once
// from `build` batches keyed by `buildKey`, then each probe chunk is matched by
// `probeKey` and **enriched** with the requested build columns (Bring). Inner join by
// default — matched probe rows are kept (compacted), unmatched dropped; call
// LeftOuter to keep unmatched probe rows with NULL brought cells instead. By default
// the build side is a **lookup table: one row per key** (later builds override) — the
// dimension-enrichment case; call MultiMatch for a full M:N relation (build keeps all
// rows per key, each probe row fans out to one output row per match).
//
// The build table is read-only after construction, so a HashJoin IS safe under
// vector.Parallel (each shard builds its own copy). Keys are Int64 or String.
//
//	sdk.New().From(stream).
//	    Apply(vector.HashJoin(dimBatches, "id", "user_id").
//	        Bring("country", "country").Bring("tier", "tier").Op()).
//	    To(vector.Collect()).Run(ctx)
//	// output rows = matched probe rows; columns = probe cols + country + tier
func HashJoin(build []*core.Batch, buildKey, probeKey string) *HJoin {
	return &HJoin{build: build, buildKey: buildKey, probeKey: probeKey}
}

type bring struct{ field, out string }

// HJoin is the build-side hash-join builder.
type HJoin struct {
	build              []*core.Batch
	buildKey, probeKey string
	brings             []bring
	leftOuter          bool
	multi              bool
}

// Bring appends build column `field` (renamed to `out`) to matched probe rows.
func (j *HJoin) Bring(field, out string) *HJoin {
	j.brings = append(j.brings, bring{field: field, out: out})
	return j
}

// LeftOuter switches the join from inner to left-outer: every probe row is kept; an
// unmatched probe row keeps its own columns and gets NULL (validity mask) in each
// brought column. Default is inner (unmatched probe rows dropped).
func (j *HJoin) LeftOuter() *HJoin {
	j.leftOuter = true
	return j
}

// MultiMatch makes the build side a full relation (M:N) instead of a one-row-per-key
// lookup table: the build keeps *every* row per key, and each probe row fans out to
// one output row per matching build row (so K build rows for a key → K output rows).
// Combine with LeftOuter for a left M:N join (a probe row with no match still emits a
// single row with NULL brought cells). Default (off) is the efficient
// dimension-enrichment path (last build row wins, output row count = matched probe
// rows, compacted in place).
func (j *HJoin) MultiMatch() *HJoin {
	j.multi = true
	return j
}

// Op builds the join operator.
func (j *HJoin) Op() core.Operator {
	return &joinOp{HJoin: *j}
}

// buildCol is one gathered build column (one typed slice per kind).
type buildCol struct {
	kind core.ColumnKind
	i64  []int64
	f64  []float64
	str  []string
	b    []bool
}

type joinOp struct {
	HJoin

	built  bool
	strKey bool
	li     map[int64]int   // single-match: key → build idx (last wins)
	ls     map[string]int  //
	mli    map[int64][]int // multi-match: key → all build idxs
	mls    map[string][]int
	nBuilt int
	cols   []buildCol // parallel to brings
}

func (o *joinOp) OnSchemaChange(core.Schema) {}

// buildTable materialises the lookup map + brought columns from the build batches.
func (o *joinOp) buildTable() error {
	if len(o.build) == 0 {
		return fmt.Errorf("vector: HashJoin requires at least one build batch")
	}
	o.cols = make([]buildCol, len(o.brings))
	for _, b := range o.build {
		ik := b.Int64(o.buildKey)
		sk := b.String(o.buildKey)
		if ik == nil && sk == nil {
			return fmt.Errorf("vector: HashJoin build key %q missing or not Int64/String", o.buildKey)
		}
		strKey := sk != nil
		if !o.built {
			o.strKey = strKey
			switch {
			case o.multi && strKey:
				o.mls = make(map[string][]int)
			case o.multi:
				o.mli = make(map[int64][]int)
			case strKey:
				o.ls = make(map[string]int)
			default:
				o.li = make(map[int64]int)
			}
			o.built = true
		} else if strKey != o.strKey {
			return fmt.Errorf("vector: HashJoin build key %q kind inconsistent across batches", o.buildKey)
		}

		// Fetch each brought field's column once; record its kind (even for 0-row
		// batches, so the output schema is typed correctly).
		fi64 := make([][]int64, len(o.brings))
		ff64 := make([][]float64, len(o.brings))
		fstr := make([][]string, len(o.brings))
		fbool := make([][]bool, len(o.brings))
		for bi, br := range o.brings {
			switch {
			case b.Int64(br.field) != nil:
				o.cols[bi].kind, fi64[bi] = core.KindInt64, b.Int64(br.field)
			case b.Float64(br.field) != nil:
				o.cols[bi].kind, ff64[bi] = core.KindFloat64, b.Float64(br.field)
			case b.String(br.field) != nil:
				o.cols[bi].kind, fstr[bi] = core.KindString, b.String(br.field)
			case b.Bool(br.field) != nil:
				o.cols[bi].kind, fbool[bi] = core.KindBool, b.Bool(br.field)
			default:
				return fmt.Errorf("vector: HashJoin build field %q not found", br.field)
			}
		}

		for row := 0; row < b.Len; row++ {
			idx := o.nBuilt
			for bi := range o.brings {
				switch o.cols[bi].kind {
				case core.KindInt64:
					o.cols[bi].i64 = append(o.cols[bi].i64, fi64[bi][row])
				case core.KindFloat64:
					o.cols[bi].f64 = append(o.cols[bi].f64, ff64[bi][row])
				case core.KindString:
					o.cols[bi].str = append(o.cols[bi].str, fstr[bi][row])
				case core.KindBool:
					o.cols[bi].b = append(o.cols[bi].b, fbool[bi][row])
				}
			}
			switch {
			case o.multi && strKey:
				o.mls[sk[row]] = append(o.mls[sk[row]], idx)
			case o.multi:
				o.mli[ik[row]] = append(o.mli[ik[row]], idx)
			case strKey:
				o.ls[sk[row]] = idx
			default:
				o.li[ik[row]] = idx
			}
			o.nBuilt++
		}
	}
	return nil
}

func (o *joinOp) Process(in []core.Record) ([]core.Record, error) {
	if !o.built {
		if err := o.buildTable(); err != nil {
			return nil, err
		}
	}
	var out []core.Record
	for _, r := range in {
		b := r.Chunk
		if b == nil {
			continue
		}
		var ikeys []int64
		var skeys []string
		if o.strKey {
			skeys = b.String(o.probeKey)
			if skeys == nil {
				return nil, fmt.Errorf("vector: HashJoin probe key %q missing or not String (build key is String)", o.probeKey)
			}
		} else {
			ikeys = b.Int64(o.probeKey)
			if ikeys == nil {
				return nil, fmt.Errorf("vector: HashJoin probe key %q missing or not Int64 (build key is Int64)", o.probeKey)
			}
		}

		if o.multi {
			out = append(out, o.processMulti(b, ikeys, skeys))
			continue
		}

		// Single-match (dimension) path. Match each probe row to one build index.
		// Inner: drop unmatched (compact in place). Left-outer: keep every row,
		// recording a -1 sentinel for unmatched so gatherBuild emits NULL brought cells.
		w := 0
		buildIdx := make([]int, 0, b.Len)
		for i := 0; i < b.Len; i++ {
			var idx int
			var ok bool
			if o.strKey {
				idx, ok = o.ls[skeys[i]]
			} else {
				idx, ok = o.li[ikeys[i]]
			}
			if !ok {
				if !o.leftOuter {
					continue
				}
				idx = -1 // no match → NULL brought cells
			}
			if w != i {
				b.CopyRow(w, i)
			}
			buildIdx = append(buildIdx, idx)
			w++
		}
		b.Truncate(w)

		// Append brought build columns, gathered by build index for kept rows.
		for bi, br := range o.brings {
			b.Cols = append(b.Cols, gatherBuild(o.cols[bi], buildIdx))
			b.Schema.Fields = append(b.Schema.Fields, core.Field{Name: br.out, Type: kindToFieldType(o.cols[bi].kind)})
		}
		out = append(out, r)
	}
	return out, nil
}

// processMulti runs the M:N fan-out path: each probe row emits one output row per
// matching build row (left-outer also emits a single NULL-brought row for no match).
// Output is a fresh batch — probe columns are gathered (repeated per match), brought
// columns gathered by build index — so the input chunk is not mutated.
func (o *joinOp) processMulti(b *core.Batch, ikeys []int64, skeys []string) core.Record {
	// Build the row plan: parallel (probeRow, buildIdx) lists, one entry per output row.
	probeRows := make([]int, 0, b.Len)
	buildIdx := make([]int, 0, b.Len)
	for i := 0; i < b.Len; i++ {
		var matches []int
		if o.strKey {
			matches = o.mls[skeys[i]]
		} else {
			matches = o.mli[ikeys[i]]
		}
		if len(matches) == 0 {
			if o.leftOuter {
				probeRows = append(probeRows, i)
				buildIdx = append(buildIdx, -1) // NULL brought cells
			}
			continue
		}
		for _, bi := range matches {
			probeRows = append(probeRows, i)
			buildIdx = append(buildIdx, bi)
		}
	}

	cols := make([]core.Column, 0, len(b.Cols)+len(o.brings))
	fields := make([]core.Field, 0, len(b.Schema.Fields)+len(o.brings))
	for ci := range b.Cols {
		cols = append(cols, gatherProbe(b.Cols[ci], probeRows))
		fields = append(fields, b.Schema.Fields[ci])
	}
	for bi, br := range o.brings {
		cols = append(cols, gatherBuild(o.cols[bi], buildIdx))
		fields = append(fields, core.Field{Name: br.out, Type: kindToFieldType(o.cols[bi].kind)})
	}
	return core.Record{Chunk: &core.Batch{Schema: core.Schema{Fields: fields}, Len: len(probeRows), Cols: cols}}
}

// gatherProbe builds a new column by selecting rows from c in the given order
// (repeats allowed, for fan-out). The null mask is carried when present.
func gatherProbe(c core.Column, rows []int) core.Column {
	n := len(rows)
	out := core.Column{Kind: c.Kind}
	switch c.Kind {
	case core.KindInt64:
		v := make([]int64, n)
		for i, r := range rows {
			v[i] = c.I64[r]
		}
		out.I64 = v
	case core.KindFloat64:
		v := make([]float64, n)
		for i, r := range rows {
			v[i] = c.F64[r]
		}
		out.F64 = v
	case core.KindString:
		v := make([]string, n)
		for i, r := range rows {
			v[i] = c.Str[r]
		}
		out.Str = v
	case core.KindBool:
		v := make([]bool, n)
		for i, r := range rows {
			v[i] = c.B[r]
		}
		out.B = v
	}
	if c.Null != nil {
		nl := make([]bool, n)
		for i, r := range rows {
			nl[i] = c.Null[r]
		}
		out.Null = nl
	}
	return out
}

// gatherBuild materialises one brought column for the kept probe rows. A build index
// of -1 (left-outer, no match) yields a NULL cell: the typed slot stays zero and a
// validity mask is allocated lazily (nil when there are no nulls — the inner-join
// case — keeping it zero-overhead).
func gatherBuild(bc buildCol, idx []int) core.Column {
	var null []bool
	markNull := func(i int) {
		if null == nil {
			null = make([]bool, len(idx))
		}
		null[i] = true
	}
	switch bc.kind {
	case core.KindFloat64:
		v := make([]float64, len(idx))
		for i, bi := range idx {
			if bi < 0 {
				markNull(i)
				continue
			}
			v[i] = bc.f64[bi]
		}
		return core.Column{Kind: core.KindFloat64, F64: v, Null: null}
	case core.KindString:
		v := make([]string, len(idx))
		for i, bi := range idx {
			if bi < 0 {
				markNull(i)
				continue
			}
			v[i] = bc.str[bi]
		}
		return core.Column{Kind: core.KindString, Str: v, Null: null}
	case core.KindBool:
		v := make([]bool, len(idx))
		for i, bi := range idx {
			if bi < 0 {
				markNull(i)
				continue
			}
			v[i] = bc.b[bi]
		}
		return core.Column{Kind: core.KindBool, B: v, Null: null}
	default: // Int64
		v := make([]int64, len(idx))
		for i, bi := range idx {
			if bi < 0 {
				markNull(i)
				continue
			}
			v[i] = bc.i64[bi]
		}
		return core.Column{Kind: core.KindInt64, I64: v, Null: null}
	}
}

func kindToFieldType(k core.ColumnKind) core.FieldType {
	switch k {
	case core.KindFloat64:
		return core.FieldTypeFloat
	case core.KindString:
		return core.FieldTypeString
	case core.KindBool:
		return core.FieldTypeBool
	default:
		return core.FieldTypeInt
	}
}
