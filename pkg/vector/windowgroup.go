package vector

import (
	"fmt"
	"sort"

	"github.com/gribovan2005/drift/pkg/core"
)

// TumblingGroup builds a vectorized, keyed, EVENT-TIME tumbling aggregation: rows
// are bucketed into windows [start, start+size) by an int64 timestamp column and
// aggregated per (window, key). It mirrors operator.EventTimeWindow but keyed and
// columnar: the watermark is maxTs − lateness; a window fires (emits during
// Process — periodic emit) once its end ≤ watermark; rows for an already-fired
// window are dropped as late. Flush fires all remaining open windows.
//
// `size` and `lateness` are int64 in the SAME unit as the timestamp column
// (e.g. epoch millis). Keys are Int64 or String; aggregates reuse GroupBy's set.
// Single-stage (per-window/per-key state) — do not wrap in vector.Parallel.
//
//	sdk.New().From(src).
//	    Apply(vector.TumblingGroup("merchant", "ts", 60000).Lateness(5000).
//	        Count("n").SumFloat64("amount", "total").Op()).
//	    To(vector.Collect()).Run(ctx)
//	// → result chunks: columns [ts(window start), merchant, n, total]
func TumblingGroup(keyField, tsField string, size int64) *WGroup {
	return &WGroup{keyField: keyField, tsField: tsField, size: size}
}

// WGroup is the tumbling group-by builder.
type WGroup struct {
	keyField, tsField string
	size, lateness    int64
	aggs              []aggSpec
}

// Lateness sets allowed lateness (same unit as the ts column). Default 0.
func (g *WGroup) Lateness(l int64) *WGroup { g.lateness = l; return g }

// Count adds count(*) per (window,key) as column out.
func (g *WGroup) Count(out string) *WGroup {
	g.aggs = append(g.aggs, aggSpec{kind: aggCount, out: out})
	return g
}

// SumInt64 adds sum(valField) over an int64 column as column out.
func (g *WGroup) SumInt64(valField, out string) *WGroup {
	g.aggs = append(g.aggs, aggSpec{kind: aggSumInt64, valField: valField, out: out})
	return g
}

// SumFloat64 adds sum(valField) over a float64 column as column out.
func (g *WGroup) SumFloat64(valField, out string) *WGroup {
	g.aggs = append(g.aggs, aggSpec{kind: aggSumFloat64, valField: valField, out: out})
	return g
}

// MaxInt64 adds max(valField) over an int64 column as column out.
func (g *WGroup) MaxInt64(valField, out string) *WGroup {
	g.aggs = append(g.aggs, aggSpec{kind: aggMaxInt64, valField: valField, out: out})
	return g
}

// Op builds the tumbling group-by operator.
func (g *WGroup) Op() core.Operator {
	return &windowOp{
		keyField: g.keyField, tsField: g.tsField, size: g.size, lateness: g.lateness,
		aggs: g.aggs, windows: make(map[int64]*winState),
	}
}

// winState holds one window's per-key accumulators (typed by key kind).
type winState struct {
	mi map[int64]*acc
	ms map[string]*acc
}

type windowOp struct {
	keyField, tsField string
	size, lateness    int64
	aggs              []aggSpec

	started     bool
	strKey      bool
	maxSeen     int64
	firedUpTo   int64 // highest watermark already fired
	firedSet    bool
	lateDropped int64
	windows     map[int64]*winState
}

func (o *windowOp) OnSchemaChange(core.Schema) {}

// windowStart aligns ts down to a size boundary (floor division, so negative ts
// bucket correctly).
func windowStart(ts, size int64) int64 {
	q := ts / size
	if ts%size != 0 && ts < 0 {
		q--
	}
	return q * size
}

func (o *windowOp) newWinState() *winState {
	if o.strKey {
		return &winState{ms: make(map[string]*acc)}
	}
	return &winState{mi: make(map[int64]*acc)}
}

func (o *windowOp) Process(in []core.Record) ([]core.Record, error) {
	if o.size <= 0 {
		return nil, fmt.Errorf("vector: TumblingGroup size must be ≥ 1, got %d", o.size)
	}
	var out []core.Record
	for _, r := range in {
		b := r.Chunk
		if b == nil {
			continue
		}
		ts := b.Int64(o.tsField)
		if ts == nil {
			return nil, fmt.Errorf("vector: TumblingGroup ts field %q not an int64 column", o.tsField)
		}
		ikeys := b.Int64(o.keyField)
		skeys := b.String(o.keyField)
		if ikeys == nil && skeys == nil {
			return nil, fmt.Errorf("vector: TumblingGroup key %q missing or not Int64/String", o.keyField)
		}
		strKey := skeys != nil
		if !o.started {
			o.strKey, o.started = strKey, true
		} else if strKey != o.strKey {
			return nil, fmt.Errorf("vector: TumblingGroup key %q kind changed between chunks", o.keyField)
		}
		i64cols, f64cols, err := fetchAggCols(b, o.aggs, "TumblingGroup")
		if err != nil {
			return nil, err
		}

		// Advance maxSeen across the whole batch first, so the watermark reflects
		// the latest event before deciding which rows are late.
		for i := 0; i < b.Len; i++ {
			if ts[i] > o.maxSeen {
				o.maxSeen = ts[i]
			}
		}
		wm := o.maxSeen - o.lateness

		for i := 0; i < b.Len; i++ {
			start := windowStart(ts[i], o.size)
			// Late: window already fired in a previous batch. Windows that close
			// within this batch are still accepted (aggregated before fireClosed).
			if o.firedSet && start+o.size <= o.firedUpTo {
				o.lateDropped++
				continue
			}
			ws := o.windows[start]
			if ws == nil {
				ws = o.newWinState()
				o.windows[start] = ws
			}
			if strKey {
				a := ws.ms[skeys[i]]
				if a == nil {
					a = newAggAcc(len(o.aggs))
					ws.ms[skeys[i]] = a
				}
				applyAggs(a, o.aggs, i, i64cols, f64cols)
			} else {
				a := ws.mi[ikeys[i]]
				if a == nil {
					a = newAggAcc(len(o.aggs))
					ws.mi[ikeys[i]] = a
				}
				applyAggs(a, o.aggs, i, i64cols, f64cols)
			}
		}

		if fired := o.fire(o.windowsClosedBy(wm)); fired != nil {
			out = append(out, *fired)
		}
		if !o.firedSet || wm > o.firedUpTo {
			o.firedUpTo, o.firedSet = wm, true
		}
	}
	return out, nil
}

// windowsClosedBy returns the starts of windows whose end ≤ wm, ascending.
func (o *windowOp) windowsClosedBy(wm int64) []int64 {
	var ready []int64
	for start := range o.windows {
		if start+o.size <= wm {
			ready = append(ready, start)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i] < ready[j] })
	return ready
}

// fire builds a result chunk for the given window starts and removes them.
func (o *windowOp) fire(starts []int64) *core.Record {
	if len(starts) == 0 {
		return nil
	}
	rec := o.buildResult(starts)
	for _, s := range starts {
		delete(o.windows, s)
	}
	return &rec
}

// buildResult materialises one columnar chunk: [tsField(window start), keyField,
// aggs...], rows ordered by (window, key).
func (o *windowOp) buildResult(starts []int64) core.Record {
	keyType := core.FieldTypeString
	if !o.strKey {
		keyType = core.FieldTypeInt
	}
	fields := append([]core.Field{
		{Name: o.tsField, Type: core.FieldTypeInt},
		{Name: o.keyField, Type: keyType},
	}, aggFields(o.aggs)...)

	var winCol, keyI64 []int64
	var keyStr []string
	var accs []*acc
	for _, start := range starts {
		ws := o.windows[start]
		if o.strKey {
			ks := make([]string, 0, len(ws.ms))
			for k := range ws.ms {
				ks = append(ks, k)
			}
			sort.Strings(ks)
			for _, k := range ks {
				winCol = append(winCol, start)
				keyStr = append(keyStr, k)
				accs = append(accs, ws.ms[k])
			}
		} else {
			ks := make([]int64, 0, len(ws.mi))
			for k := range ws.mi {
				ks = append(ks, k)
			}
			sort.Slice(ks, func(i, j int) bool { return ks[i] < ks[j] })
			for _, k := range ks {
				winCol = append(winCol, start)
				keyI64 = append(keyI64, k)
				accs = append(accs, ws.mi[k])
			}
		}
	}

	cols := make([]core.Column, len(fields))
	cols[0] = core.Column{Kind: core.KindInt64, I64: winCol}
	if keyType == core.FieldTypeInt {
		cols[1] = core.Column{Kind: core.KindInt64, I64: keyI64}
	} else {
		if keyStr == nil {
			keyStr = []string{}
		}
		cols[1] = core.Column{Kind: core.KindString, Str: keyStr}
	}
	copy(cols[2:], aggColumns(o.aggs, accs))
	return core.Record{Chunk: &core.Batch{Schema: core.Schema{Fields: fields}, Len: len(accs), Cols: cols}}
}

// Flush fires all remaining open windows (ascending).
func (o *windowOp) Flush() ([]core.Record, error) {
	if len(o.windows) == 0 {
		return nil, nil
	}
	all := make([]int64, 0, len(o.windows))
	for start := range o.windows {
		all = append(all, start)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	if rec := o.fire(all); rec != nil {
		return []core.Record{*rec}, nil
	}
	return nil, nil
}
