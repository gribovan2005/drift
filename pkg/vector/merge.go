package vector

import (
	"fmt"

	"github.com/gribovan2005/drift/pkg/core"
)

// MergeOp builds an operator that merges partial GroupBy result chunks — one per
// independent lane — into the single global result, re-aggregating by key with each
// aggregate's combine rule: Count and Sum columns are summed, Max is maxed. Feed it
// the partial result chunks produced by this same Group's Op() running in each lane;
// it emits the global result on Flush. This makes an *unsharded*, distributed GroupBy
// across N lanes (see pipeline.RunLanes) global-correct regardless of how the input
// is distributed across lanes — no key/partition sharding required.
//
//	gb := vector.GroupBy("merchant").Count("n").SumFloat64("amount", "total")
//	// in each lane:  Apply(gb.Op())        → that lane's partial result chunk
//	// then merge:    Apply(gb.MergeOp())   → the global result chunk
//
// The combine rules are exact because every supported aggregate is associative:
// count/sum compose by addition, max by maximum. (Average etc. would need partial
// count+sum decomposition — not currently offered.)
func (g *Group) MergeOp() core.Operator {
	return &mergeOp{keyField: g.keyField, aggs: g.aggs}
}

// mergeOp folds partial GroupBy result chunks by key. It carries the same state as
// groupOp and shares flushGroups, so its output schema is byte-identical to a single
// global GroupBy over the whole input.
type mergeOp struct {
	keyField string
	aggs     []aggSpec

	started bool
	strKey  bool
	mi      map[int64]*acc
	ms      map[string]*acc
}

func (o *mergeOp) OnSchemaChange(core.Schema) {}

func (o *mergeOp) Process(in []core.Record) ([]core.Record, error) {
	for _, r := range in {
		b := r.Chunk
		if b == nil || b.Len == 0 {
			// An empty partial (e.g. a lane that received no input) carries no keys
			// and defaults to a string key column — skip it so it neither contributes
			// nor fixes the merge's key kind.
			continue
		}
		ikeys := b.Int64(o.keyField)
		skeys := b.String(o.keyField)
		if ikeys == nil && skeys == nil {
			return nil, fmt.Errorf("vector: MergeGroups key %q missing or not Int64/String", o.keyField)
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
			return nil, fmt.Errorf("vector: MergeGroups key %q kind changed between chunks", o.keyField)
		}

		// Resolve each aggregate's partial column by its output name (the column the
		// per-lane GroupBy produced), typed by the aggregate's kind.
		i64cols, f64cols, err := o.fetchPartialCols(b)
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
				o.mergeRow(a, i, i64cols, f64cols)
			}
		} else {
			for i := 0; i < b.Len; i++ {
				a := o.mi[ikeys[i]]
				if a == nil {
					a = newAggAcc(len(o.aggs))
					o.mi[ikeys[i]] = a
				}
				o.mergeRow(a, i, i64cols, f64cols)
			}
		}
	}
	return nil, nil
}

// fetchPartialCols resolves the partial column for each aggregate from b by its out
// name (once per chunk). Float64 sums read a float64 column; Count/SumInt64/MaxInt64
// read an int64 column.
func (o *mergeOp) fetchPartialCols(b *core.Batch) ([][]int64, [][]float64, error) {
	i64cols := make([][]int64, len(o.aggs))
	f64cols := make([][]float64, len(o.aggs))
	for j, a := range o.aggs {
		if a.kind == aggSumFloat64 {
			c := b.Float64(a.out)
			if c == nil {
				return nil, nil, fmt.Errorf("vector: MergeGroups partial col %q not a float64 column", a.out)
			}
			f64cols[j] = c
		} else {
			c := b.Int64(a.out)
			if c == nil {
				return nil, nil, fmt.Errorf("vector: MergeGroups partial col %q not an int64 column", a.out)
			}
			i64cols[j] = c
		}
	}
	return i64cols, f64cols, nil
}

// mergeRow folds row i of the partial columns into accumulator a using each
// aggregate's combine rule. Count partials accumulate into a.count so flushGroups
// (shared with groupOp) renders them identically.
func (o *mergeOp) mergeRow(a *acc, i int, i64cols [][]int64, f64cols [][]float64) {
	for j, ag := range o.aggs {
		switch ag.kind {
		case aggCount:
			a.count += i64cols[j][i]
		case aggSumInt64:
			a.i64[j] += i64cols[j][i]
		case aggSumFloat64:
			a.f64[j] += f64cols[j][i]
		case aggMaxInt64:
			v := i64cols[j][i]
			if !a.seen[j] || v > a.i64[j] {
				a.i64[j], a.seen[j] = v, true
			}
		}
	}
}

func (o *mergeOp) Flush() ([]core.Record, error) {
	return flushGroups(o.keyField, o.aggs, o.started, o.strKey, o.mi, o.ms)
}
