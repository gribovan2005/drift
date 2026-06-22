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

func (o *groupOp) newAcc() *acc {
	return &acc{i64: make([]int64, len(o.aggs)), f64: make([]float64, len(o.aggs)), seen: make([]bool, len(o.aggs))}
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

		// Fetch each agg's value column once per chunk.
		i64cols := make([][]int64, len(o.aggs))
		f64cols := make([][]float64, len(o.aggs))
		for j, a := range o.aggs {
			switch a.kind {
			case aggSumInt64, aggMaxInt64:
				c := b.Int64(a.valField)
				if c == nil {
					return nil, fmt.Errorf("vector: GroupBy agg field %q not an int64 column", a.valField)
				}
				i64cols[j] = c
			case aggSumFloat64:
				c := b.Float64(a.valField)
				if c == nil {
					return nil, fmt.Errorf("vector: GroupBy agg field %q not a float64 column", a.valField)
				}
				f64cols[j] = c
			}
		}

		if strKey {
			for i := 0; i < b.Len; i++ {
				a := o.ms[skeys[i]]
				if a == nil {
					a = o.newAcc()
					o.ms[skeys[i]] = a
				}
				o.update(a, i, i64cols, f64cols)
			}
		} else {
			for i := 0; i < b.Len; i++ {
				a := o.mi[ikeys[i]]
				if a == nil {
					a = o.newAcc()
					o.mi[ikeys[i]] = a
				}
				o.update(a, i, i64cols, f64cols)
			}
		}
	}
	return nil, nil
}

func (o *groupOp) update(a *acc, i int, i64cols [][]int64, f64cols [][]float64) {
	a.count++
	for j, ag := range o.aggs {
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

func (o *groupOp) Flush() ([]core.Record, error) {
	// Schema: key column + one column per aggregate.
	fields := make([]core.Field, 0, 1+len(o.aggs))
	keyType := core.FieldTypeString
	if o.started && !o.strKey {
		keyType = core.FieldTypeInt
	}
	fields = append(fields, core.Field{Name: o.keyField, Type: keyType})
	for _, a := range o.aggs {
		t := core.FieldTypeInt
		if a.kind == aggSumFloat64 {
			t = core.FieldTypeFloat
		}
		fields = append(fields, core.Field{Name: a.out, Type: t})
	}

	// Collect rows in sorted key order, gathering each key's accumulator.
	var n int
	var keyI64 []int64
	var keyStr []string
	accs := []*acc{}
	if o.strKey {
		keyStr = make([]string, 0, len(o.ms))
		for k := range o.ms {
			keyStr = append(keyStr, k)
		}
		sort.Strings(keyStr)
		n = len(keyStr)
		for _, k := range keyStr {
			accs = append(accs, o.ms[k])
		}
	} else {
		keyI64 = make([]int64, 0, len(o.mi))
		for k := range o.mi {
			keyI64 = append(keyI64, k)
		}
		sort.Slice(keyI64, func(a, b int) bool { return keyI64[a] < keyI64[b] })
		n = len(keyI64)
		for _, k := range keyI64 {
			accs = append(accs, o.mi[k])
		}
	}

	cols := make([]core.Column, len(fields))
	// key column
	if keyType == core.FieldTypeInt {
		cols[0] = core.Column{Kind: core.KindInt64, I64: keyI64}
	} else {
		if keyStr == nil {
			keyStr = []string{}
		}
		cols[0] = core.Column{Kind: core.KindString, Str: keyStr}
	}
	// aggregate columns
	for j, a := range o.aggs {
		switch a.kind {
		case aggSumFloat64:
			v := make([]float64, n)
			for i, ac := range accs {
				v[i] = ac.f64[j]
			}
			cols[j+1] = core.Column{Kind: core.KindFloat64, F64: v}
		case aggCount:
			v := make([]int64, n)
			for i, ac := range accs {
				v[i] = ac.count
			}
			cols[j+1] = core.Column{Kind: core.KindInt64, I64: v}
		default: // SumInt64 / MaxInt64
			v := make([]int64, n)
			for i, ac := range accs {
				v[i] = ac.i64[j]
			}
			cols[j+1] = core.Column{Kind: core.KindInt64, I64: v}
		}
	}

	return []core.Record{{Chunk: &core.Batch{Schema: core.Schema{Fields: fields}, Len: n, Cols: cols}}}, nil
}
