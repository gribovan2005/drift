package vector

import (
	"fmt"
	"math"
	"sort"

	"github.com/gribovan2005/drift/pkg/core"
)

// SessionGroup builds a vectorized, keyed, EVENT-TIME session aggregation: per key,
// rows are grouped into sessions of activity separated by gaps of inactivity. A row at
// ts extends a session if ts ∈ [min-gap, max+gap]; otherwise it opens a new session,
// and sessions that come within gap of each other merge. A session fires once the
// watermark (maxTs − lateness) reaches its max+gap — nothing more can extend it.
// Mirrors operator.SessionWindow, but keyed-columnar with combinable accumulators
// (no per-row buffering): merging two sessions folds their accumulators (Count/Sum
// add, Max maxes).
//
// `gap` and `lateness` are int64 in the SAME unit as the ts column. Keys are
// Int64/String; aggregates reuse GroupBy's set. Single-stage — do not wrap in
// vector.Parallel.
//
//	sdk.New().From(src).
//	    Apply(vector.SessionGroup("user", "ts", 30000).Count("n").Op()).
//	    To(vector.Collect()).Run(ctx)
//	// → result chunks: columns [ts(session start), user, n], a row per (session,key)
func SessionGroup(keyField, tsField string, gap int64) *SGroup {
	return &SGroup{keyField: keyField, tsField: tsField, gap: gap}
}

// SGroup is the session group-by builder.
type SGroup struct {
	keyField, tsField string
	gap, lateness     int64
	aggs              []aggSpec
}

// Lateness sets allowed lateness (same unit as the ts column). Default 0.
func (g *SGroup) Lateness(l int64) *SGroup { g.lateness = l; return g }

// Count adds count(*) per session as column out.
func (g *SGroup) Count(out string) *SGroup {
	g.aggs = append(g.aggs, aggSpec{kind: aggCount, out: out})
	return g
}

// SumInt64 adds sum(valField) over an int64 column as column out.
func (g *SGroup) SumInt64(valField, out string) *SGroup {
	g.aggs = append(g.aggs, aggSpec{kind: aggSumInt64, valField: valField, out: out})
	return g
}

// SumFloat64 adds sum(valField) over a float64 column as column out.
func (g *SGroup) SumFloat64(valField, out string) *SGroup {
	g.aggs = append(g.aggs, aggSpec{kind: aggSumFloat64, valField: valField, out: out})
	return g
}

// MaxInt64 adds max(valField) over an int64 column as column out.
func (g *SGroup) MaxInt64(valField, out string) *SGroup {
	g.aggs = append(g.aggs, aggSpec{kind: aggMaxInt64, valField: valField, out: out})
	return g
}

// Op builds the session group-by operator.
func (g *SGroup) Op() core.Operator {
	return &sessionOp{
		keyField: g.keyField, tsField: g.tsField, gap: g.gap, lateness: g.lateness,
		aggs: g.aggs, si: make(map[int64][]*vsess), ss: make(map[string][]*vsess),
	}
}

// vsess is one open session: its event-time span and combined accumulator.
type vsess struct {
	min, max int64
	acc      *acc
}

type sessionOp struct {
	keyField, tsField string
	gap, lateness     int64
	aggs              []aggSpec

	started     bool
	strKey      bool
	maxSeen     int64
	firedUpTo   int64 // highest watermark already fired
	firedSet    bool
	lateDropped int64
	si          map[int64][]*vsess
	ss          map[string][]*vsess
}

func (o *sessionOp) OnSchemaChange(core.Schema) {}

func (o *sessionOp) Process(in []core.Record) ([]core.Record, error) {
	if o.gap <= 0 {
		return nil, fmt.Errorf("vector: SessionGroup gap must be ≥ 1, got %d", o.gap)
	}
	var out []core.Record
	for _, r := range in {
		b := r.Chunk
		if b == nil {
			continue
		}
		ts := b.Int64(o.tsField)
		if ts == nil {
			return nil, fmt.Errorf("vector: SessionGroup ts field %q not an int64 column", o.tsField)
		}
		ikeys := b.Int64(o.keyField)
		skeys := b.String(o.keyField)
		if ikeys == nil && skeys == nil {
			return nil, fmt.Errorf("vector: SessionGroup key %q missing or not Int64/String", o.keyField)
		}
		strKey := skeys != nil
		if !o.started {
			o.strKey, o.started = strKey, true
		} else if strKey != o.strKey {
			return nil, fmt.Errorf("vector: SessionGroup key %q kind changed between chunks", o.keyField)
		}
		i64cols, f64cols, err := fetchAggCols(b, o.aggs, "SessionGroup")
		if err != nil {
			return nil, err
		}

		// Advance the watermark across the whole batch before deciding lateness.
		for i := 0; i < b.Len; i++ {
			if ts[i] > o.maxSeen {
				o.maxSeen = ts[i]
			}
		}
		wm := o.maxSeen - o.lateness

		for i := 0; i < b.Len; i++ {
			if strKey {
				o.ss[skeys[i]] = o.insert(o.ss[skeys[i]], ts[i], i, i64cols, f64cols)
			} else {
				o.si[ikeys[i]] = o.insert(o.si[ikeys[i]], ts[i], i, i64cols, f64cols)
			}
		}

		if rec := o.collect(wm, false); rec != nil {
			out = append(out, *rec)
		}
		if !o.firedSet || wm > o.firedUpTo {
			o.firedUpTo, o.firedSet = wm, true
		}
	}
	return out, nil
}

// insert folds row i into the key's session list: extend a session whose span comes
// within gap of ts, else open a new one (dropped+counted if too late to ever fire);
// adjacent sessions are then merged.
func (o *sessionOp) insert(list []*vsess, ts int64, i int, i64cols [][]int64, f64cols [][]float64) []*vsess {
	for _, s := range list {
		if ts >= s.min-o.gap && ts <= s.max+o.gap {
			applyAggs(s.acc, o.aggs, i, i64cols, f64cols)
			if ts < s.min {
				s.min = ts
			}
			if ts > s.max {
				s.max = ts
			}
			return o.mergeAdjacent(list)
		}
	}
	// New session. Late if it could never fire (its own end is already ≤ the last
	// fired watermark).
	if o.firedSet && ts+o.gap <= o.firedUpTo {
		o.lateDropped++
		return list
	}
	ns := &vsess{min: ts, max: ts, acc: newAggAcc(len(o.aggs))}
	applyAggs(ns.acc, o.aggs, i, i64cols, f64cols)
	return o.mergeAdjacent(append(list, ns))
}

// mergeAdjacent sorts a key's sessions by start and merges any within gap, folding
// their accumulators.
func (o *sessionOp) mergeAdjacent(list []*vsess) []*vsess {
	if len(list) < 2 {
		return list
	}
	sort.Slice(list, func(i, j int) bool { return list[i].min < list[j].min })
	merged := list[:1]
	for _, s := range list[1:] {
		last := merged[len(merged)-1]
		if s.min <= last.max+o.gap { // within gap → merge into last
			combineAcc(last.acc, s.acc, o.aggs)
			if s.max > last.max {
				last.max = s.max
			}
			if s.min < last.min {
				last.min = s.min
			}
		} else {
			merged = append(merged, s)
		}
	}
	return merged
}

// firedSess is one session selected to fire, carrying its key for ordering/output.
type firedSess struct {
	keyI int64
	keyS string
	min  int64
	acc  *acc
}

// collect fires sessions whose max+gap ≤ wm (or all, when flush) across keys, removing
// them, and returns one result chunk [tsField(start), keyField, aggs...] ordered by
// (start, key). Returns nil if nothing fires.
func (o *sessionOp) collect(wm int64, flush bool) *core.Record {
	closed := func(s *vsess) bool { return flush || s.max <= wm-o.gap }
	var fired []firedSess
	if o.strKey {
		for k, list := range o.ss {
			kept := list[:0]
			for _, s := range list {
				if closed(s) {
					fired = append(fired, firedSess{keyS: k, min: s.min, acc: s.acc})
				} else {
					kept = append(kept, s)
				}
			}
			if len(kept) == 0 {
				delete(o.ss, k)
			} else {
				o.ss[k] = kept
			}
		}
	} else {
		for k, list := range o.si {
			kept := list[:0]
			for _, s := range list {
				if closed(s) {
					fired = append(fired, firedSess{keyI: k, min: s.min, acc: s.acc})
				} else {
					kept = append(kept, s)
				}
			}
			if len(kept) == 0 {
				delete(o.si, k)
			} else {
				o.si[k] = kept
			}
		}
	}
	if len(fired) == 0 {
		return nil
	}
	sort.Slice(fired, func(i, j int) bool {
		if fired[i].min != fired[j].min {
			return fired[i].min < fired[j].min
		}
		if o.strKey {
			return fired[i].keyS < fired[j].keyS
		}
		return fired[i].keyI < fired[j].keyI
	})

	keyType := core.FieldTypeString
	if !o.strKey {
		keyType = core.FieldTypeInt
	}
	fields := append([]core.Field{
		{Name: o.tsField, Type: core.FieldTypeInt},
		{Name: o.keyField, Type: keyType},
	}, aggFields(o.aggs)...)

	n := len(fired)
	winCol := make([]int64, n)
	accs := make([]*acc, n)
	var keyI64 []int64
	var keyStr []string
	if o.strKey {
		keyStr = make([]string, n)
	} else {
		keyI64 = make([]int64, n)
	}
	for idx, f := range fired {
		winCol[idx] = f.min
		accs[idx] = f.acc
		if o.strKey {
			keyStr[idx] = f.keyS
		} else {
			keyI64[idx] = f.keyI
		}
	}

	cols := make([]core.Column, len(fields))
	cols[0] = core.Column{Kind: core.KindInt64, I64: winCol}
	if o.strKey {
		cols[1] = core.Column{Kind: core.KindString, Str: keyStr}
	} else {
		cols[1] = core.Column{Kind: core.KindInt64, I64: keyI64}
	}
	copy(cols[2:], aggColumns(o.aggs, accs))
	return &core.Record{Chunk: &core.Batch{Schema: core.Schema{Fields: fields}, Len: n, Cols: cols}}
}

// Flush fires all remaining open sessions (ascending start, then key).
func (o *sessionOp) Flush() ([]core.Record, error) {
	if rec := o.collect(math.MaxInt64, true); rec != nil {
		return []core.Record{*rec}, nil
	}
	return nil, nil
}
