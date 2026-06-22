package vector

import (
	"fmt"
	"sort"

	"github.com/gribovan2005/drift/pkg/core"
)

// GroupBy builds a vectorized, keyed, global GROUP BY: records are grouped by the
// named key column (Int64 or String), aggregated per key, and emitted on Flush as
// a single columnar result chunk whose Batch has the key column plus one column per
// aggregate (keys in sorted order). It is a core.Operator + core.Flusher.
//
// Global = it accumulates over the whole stream and emits once at end-of-stream
// (windowed keyed group-by is future work). Single-stage: do NOT wrap in
// vector.Parallel — each shard would emit its own partials.
//
//	sdk.New().From(src).
//	    Apply(vector.GroupBy("merchant").Count("n").SumFloat64("amount", "total").Op()).
//	    To(vector.Collect()).Run(ctx)
func GroupBy(keyField string) *Group { return &Group{keyField: keyField} }

// Group is the GROUP BY builder. Chain aggregates, then call Op.
type Group struct {
	keyField string
	aggs     []aggSpec
}

type aggKind uint8

const (
	aggCount aggKind = iota
	aggSumInt64
	aggSumFloat64
	aggMaxInt64
)

type aggSpec struct {
	kind     aggKind
	valField string // empty for Count
	out      string
}

// Count adds count(*) per key as column `out`.
func (g *Group) Count(out string) *Group {
	g.aggs = append(g.aggs, aggSpec{kind: aggCount, out: out})
	return g
}

// SumInt64 adds sum(valField) over an int64 column as column `out`.
func (g *Group) SumInt64(valField, out string) *Group {
	g.aggs = append(g.aggs, aggSpec{kind: aggSumInt64, valField: valField, out: out})
	return g
}

// SumFloat64 adds sum(valField) over a float64 column as column `out`.
func (g *Group) SumFloat64(valField, out string) *Group {
	g.aggs = append(g.aggs, aggSpec{kind: aggSumFloat64, valField: valField, out: out})
	return g
}

// MaxInt64 adds max(valField) over an int64 column as column `out`.
func (g *Group) MaxInt64(valField, out string) *Group {
	g.aggs = append(g.aggs, aggSpec{kind: aggMaxInt64, valField: valField, out: out})
	return g
}

// Op builds the group-by operator.
func (g *Group) Op() core.Operator {
	return &groupOp{keyField: g.keyField, aggs: g.aggs}
}

// acc holds one key's running aggregates. i64[j]/f64[j] are used per agg j by kind;
// seen[j] seeds MaxInt64. count is shared by all Count aggs.
type acc struct {
	count int64
	i64   []int64
	f64   []float64
	seen  []bool
}

type groupOp struct {
	keyField string
	aggs     []aggSpec

	started bool
	strKey  bool // true → string key (ms), false → int64 key (mi)
	mi      map[int64]*acc
	ms      map[string]*acc
}

// ── shared aggregate helpers (used by groupOp and windowOp) ────────────────

// newAggAcc allocates an accumulator sized for n aggregates.
func newAggAcc(n int) *acc {
	return &acc{i64: make([]int64, n), f64: make([]float64, n), seen: make([]bool, n)}
}

// fetchAggCols resolves each aggregate's value column from b (once per chunk).
// who names the operator for error messages.
func fetchAggCols(b *core.Batch, aggs []aggSpec, who string) ([][]int64, [][]float64, error) {
	i64cols := make([][]int64, len(aggs))
	f64cols := make([][]float64, len(aggs))
	for j, a := range aggs {
		switch a.kind {
		case aggSumInt64, aggMaxInt64:
			c := b.Int64(a.valField)
			if c == nil {
				return nil, nil, fmt.Errorf("vector: %s agg field %q not an int64 column", who, a.valField)
			}
			i64cols[j] = c
		case aggSumFloat64:
			c := b.Float64(a.valField)
			if c == nil {
				return nil, nil, fmt.Errorf("vector: %s agg field %q not a float64 column", who, a.valField)
			}
			f64cols[j] = c
		}
	}
	return i64cols, f64cols, nil
}

// applyAggs folds row i of the value columns into accumulator a.
func applyAggs(a *acc, aggs []aggSpec, i int, i64cols [][]int64, f64cols [][]float64) {
	a.count++
	for j, ag := range aggs {
		switch ag.kind {
		case aggSumInt64:
			a.i64[j] += i64cols[j][i]
		case aggSumFloat64:
			a.f64[j] += f64cols[j][i]
		case aggMaxInt64:
			v := i64cols[j][i]
			if !a.seen[j] || v > a.i64[j] {
				a.i64[j], a.seen[j] = v, true
			}
		case aggCount:
			// counted via a.count
		}
	}
}

// aggFields returns the output Field for each aggregate column.
func aggFields(aggs []aggSpec) []core.Field {
	out := make([]core.Field, len(aggs))
	for j, a := range aggs {
		t := core.FieldTypeInt
		if a.kind == aggSumFloat64 {
			t = core.FieldTypeFloat
		}
		out[j] = core.Field{Name: a.out, Type: t}
	}
	return out
}

// aggColumns builds one column per aggregate from accumulators in row order.
func aggColumns(aggs []aggSpec, accs []*acc) []core.Column {
	cols := make([]core.Column, len(aggs))
	n := len(accs)
	for j, a := range aggs {
		switch a.kind {
		case aggSumFloat64:
			v := make([]float64, n)
			for i, ac := range accs {
				v[i] = ac.f64[j]
			}
			cols[j] = core.Column{Kind: core.KindFloat64, F64: v}
		case aggCount:
			v := make([]int64, n)
			for i, ac := range accs {
				v[i] = ac.count
			}
			cols[j] = core.Column{Kind: core.KindInt64, I64: v}
		default: // SumInt64 / MaxInt64
			v := make([]int64, n)
			for i, ac := range accs {
				v[i] = ac.i64[j]
			}
			cols[j] = core.Column{Kind: core.KindInt64, I64: v}
		}
	}
	return cols
}

func (o *groupOp) OnSchemaChange(core.Schema) {}

func (o *groupOp) Process(in []core.Record) ([]core.Record, error) {
	for _, r := range in {
		b := r.Chunk
		if b == nil {
			continue
		}
		ikeys := b.Int64(o.keyField)
		skeys := b.String(o.keyField)
		if ikeys == nil && skeys == nil {
			return nil, fmt.Errorf("vector: GroupBy key %q missing or not Int64/String", o.keyField)
		}
		strKey := skeys != nil
		if !o.started {
			o.strKey = strKey
			if strKey {
				o.ms = make(map[string]*acc)
			} else {
				o.mi = make(map[int64]*acc)
			}
			o.started = true
		} else if strKey != o.strKey {
			return nil, fmt.Errorf("vector: GroupBy key %q kind changed between chunks", o.keyField)
		}

		i64cols, f64cols, err := fetchAggCols(b, o.aggs, "GroupBy")
		if err != nil {
			return nil, err
		}

		if strKey {
			for i := 0; i < b.Len; i++ {
				a := o.ms[skeys[i]]
				if a == nil {
					a = newAggAcc(len(o.aggs))
					o.ms[skeys[i]] = a
				}
				applyAggs(a, o.aggs, i, i64cols, f64cols)
			}
		} else {
			for i := 0; i < b.Len; i++ {
				a := o.mi[ikeys[i]]
				if a == nil {
					a = newAggAcc(len(o.aggs))
					o.mi[ikeys[i]] = a
				}
				applyAggs(a, o.aggs, i, i64cols, f64cols)
			}
		}
	}
	return nil, nil
}

func (o *groupOp) Flush() ([]core.Record, error) {
	return flushGroups(o.keyField, o.aggs, o.started, o.strKey, o.mi, o.ms)
}

// flushGroups emits one columnar result chunk from per-key accumulators: the key
// column (sorted) plus one column per aggregate. Shared by groupOp and mergeOp,
// which carry identical state.
func flushGroups(keyField string, aggs []aggSpec, started, strKey bool, mi map[int64]*acc, ms map[string]*acc) ([]core.Record, error) {
	// Schema: key column + one column per aggregate.
	keyType := core.FieldTypeString
	if started && !strKey {
		keyType = core.FieldTypeInt
	}
	fields := append([]core.Field{{Name: keyField, Type: keyType}}, aggFields(aggs)...)

	// Collect rows in sorted key order, gathering each key's accumulator.
	var n int
	var keyI64 []int64
	var keyStr []string
	accs := []*acc{}
	if strKey {
		keyStr = make([]string, 0, len(ms))
		for k := range ms {
			keyStr = append(keyStr, k)
		}
		sort.Strings(keyStr)
		n = len(keyStr)
		for _, k := range keyStr {
			accs = append(accs, ms[k])
		}
	} else {
		keyI64 = make([]int64, 0, len(mi))
		for k := range mi {
			keyI64 = append(keyI64, k)
		}
		sort.Slice(keyI64, func(a, b int) bool { return keyI64[a] < keyI64[b] })
		n = len(keyI64)
		for _, k := range keyI64 {
			accs = append(accs, mi[k])
		}
	}

	cols := make([]core.Column, len(fields))
	if keyType == core.FieldTypeInt {
		cols[0] = core.Column{Kind: core.KindInt64, I64: keyI64}
	} else {
		if keyStr == nil {
			keyStr = []string{}
		}
		cols[0] = core.Column{Kind: core.KindString, Str: keyStr}
	}
	copy(cols[1:], aggColumns(aggs, accs))

	return []core.Record{{Chunk: &core.Batch{Schema: core.Schema{Fields: fields}, Len: n, Cols: cols}}}, nil
}
