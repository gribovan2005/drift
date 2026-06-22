package vector

import (
	"fmt"

	"github.com/gribovan2005/drift/pkg/core"
)

// StreamJoin builds a vectorized, event-time, stream-stream INTERVAL equi-join over a
// single mixed stream of chunk-records (Drift's model: both sides arrive interleaved
// on one input; the DAG executor's fan-in merges multiple predecessors into it). A
// chunk is assigned to the left or right side by its Batch.Schema.ID. Rows match when
// their keyField values are equal AND their tsField values are within `window`
// (|ts_left − ts_right| ≤ window). Each side is buffered per key; the watermark
// (max ts seen across both sides − lateness) evicts buffered rows that can no longer
// match (ts < watermark − window) and drops late arrivals. Inner join, emitted eagerly
// (not a Flusher).
//
// Output columns = all left columns, then every right column except the (redundant)
// right key; a right column whose name collides with a left column is suffixed "_r".
// Keys are Int64 or String (consistent across sides); tsField is an int64 column.
// Single-stage — do not wrap in vector.Parallel.
//
//	sdk.New().From(mixed).
//	    Apply(vector.StreamJoin("orders", "shipments", "oid", "ts", 60000).Lateness(5000).Op()).
//	    To(vector.Collect()).Run(ctx)
func StreamJoin(leftID, rightID, keyField, tsField string, window int64) *SJoin {
	return &SJoin{leftID: leftID, rightID: rightID, keyField: keyField, tsField: tsField, window: window}
}

// SJoin is the stream-stream interval-join builder.
type SJoin struct {
	leftID, rightID   string
	keyField, tsField string
	window, lateness  int64
}

// Lateness sets allowed lateness (same unit as the ts column). Default 0.
func (j *SJoin) Lateness(l int64) *SJoin { j.lateness = l; return j }

// Op builds the stream-join operator.
func (j *SJoin) Op() core.Operator {
	return &sjoinOp{
		leftID: j.leftID, rightID: j.rightID, keyField: j.keyField, tsField: j.tsField,
		window: j.window, lateness: j.lateness,
		lbuf: make(map[jkey][]bufRow), rbuf: make(map[jkey][]bufRow),
	}
}

// cell is one typed value (no interface boxing), used to buffer join state and rebuild
// output columns.
type cell struct {
	kind core.ColumnKind
	i    int64
	f    float64
	s    string
	b    bool
	null bool
}

// jkey is a comparable key usable for both Int64 and String key columns (one is zero).
type jkey struct {
	i int64
	s string
}

// bufRow is one buffered row: its event time and full set of column cells.
type bufRow struct {
	ts    int64
	cells []cell
}

type sjoinOp struct {
	leftID, rightID   string
	keyField, tsField string
	window, lateness  int64

	started     bool
	strKey      bool
	maxSeen     int64
	lateDropped int64

	lbuf map[jkey][]bufRow
	rbuf map[jkey][]bufRow

	leftFields, rightFields []core.Field
	haveLeft, haveRight     bool

	outFields []core.Field
	rightOut  []int // right field indices included in the output (right key dropped)
	outReady  bool

	// accumulated matches for the current Process call (left cells, right cells).
	pendL, pendR [][]cell
}

func (o *sjoinOp) OnSchemaChange(core.Schema) {}

func (o *sjoinOp) Process(in []core.Record) ([]core.Record, error) {
	if o.leftID == o.rightID {
		return nil, fmt.Errorf("vector: StreamJoin left and right Schema.ID must differ (got %q)", o.leftID)
	}
	if o.window < 0 || o.lateness < 0 {
		return nil, fmt.Errorf("vector: StreamJoin window/lateness must be ≥ 0")
	}
	o.pendL, o.pendR = o.pendL[:0], o.pendR[:0]

	for _, r := range in {
		b := r.Chunk
		if b == nil {
			continue
		}
		isLeft := b.Schema.ID == o.leftID
		isRight := b.Schema.ID == o.rightID
		if !isLeft && !isRight {
			continue // not part of this join
		}
		if err := o.processBatch(b, isLeft); err != nil {
			return nil, err
		}
	}

	o.evict()

	if len(o.pendL) == 0 {
		return nil, nil
	}
	return []core.Record{o.buildOutput()}, nil
}

func (o *sjoinOp) processBatch(b *core.Batch, isLeft bool) error {
	ikeys := b.Int64(o.keyField)
	skeys := b.String(o.keyField)
	if ikeys == nil && skeys == nil {
		return fmt.Errorf("vector: StreamJoin key %q missing or not Int64/String", o.keyField)
	}
	strKey := skeys != nil
	if !o.started {
		o.strKey, o.started = strKey, true
	} else if strKey != o.strKey {
		return fmt.Errorf("vector: StreamJoin key %q kind inconsistent across sides/chunks", o.keyField)
	}
	ts := b.Int64(o.tsField)
	if ts == nil {
		return fmt.Errorf("vector: StreamJoin ts field %q not an int64 column", o.tsField)
	}

	// Capture each side's schema once (needed to build the output columns).
	if isLeft && !o.haveLeft {
		o.leftFields = append([]core.Field(nil), b.Schema.Fields...)
		o.haveLeft = true
	}
	if !isLeft && !o.haveRight {
		o.rightFields = append([]core.Field(nil), b.Schema.Fields...)
		o.haveRight = true
	}

	this, other := o.rbuf, o.lbuf // arriving=right → buffer right, match left
	if isLeft {
		this, other = o.lbuf, o.rbuf
	}

	// Lateness is judged against the watermark established by PRIOR batches, so rows
	// arriving in the same batch as a later timestamp aren't prematurely dropped. The
	// high-water mark advances as we scan; eviction at end of Process uses the new one.
	wm := o.maxSeen - o.lateness

	for i := 0; i < b.Len; i++ {
		if ts[i] > o.maxSeen {
			o.maxSeen = ts[i]
		}
		var k jkey
		if strKey {
			k = jkey{s: skeys[i]}
		} else {
			k = jkey{i: ikeys[i]}
		}
		t := ts[i]
		if t < wm-o.window {
			o.lateDropped++ // too late to ever match (older than the committed watermark)
			continue
		}
		// Match against the opposite side's buffer within the interval.
		for _, br := range other[k] {
			if absInt64(t-br.ts) <= o.window {
				rc := rowCells(b, i)
				if isLeft {
					o.pendL = append(o.pendL, rc)
					o.pendR = append(o.pendR, br.cells)
				} else {
					o.pendL = append(o.pendL, br.cells)
					o.pendR = append(o.pendR, rc)
				}
			}
		}
		// Buffer this row for future opposite-side arrivals.
		this[k] = append(this[k], bufRow{ts: t, cells: rowCells(b, i)})
	}
	return nil
}

// evict drops buffered rows that can no longer match any future row: ts < wm − window.
func (o *sjoinOp) evict() {
	if !o.started {
		return
	}
	wm := o.maxSeen - o.lateness
	cutoff := wm - o.window
	for _, buf := range []map[jkey][]bufRow{o.lbuf, o.rbuf} {
		for k, rows := range buf {
			kept := rows[:0]
			for _, r := range rows {
				if r.ts >= cutoff {
					kept = append(kept, r)
				}
			}
			if len(kept) == 0 {
				delete(buf, k)
			} else {
				buf[k] = kept
			}
		}
	}
}

// buildOutput materialises one chunk from the accumulated matched pairs.
func (o *sjoinOp) buildOutput() core.Record {
	if !o.outReady {
		o.computeOutFields()
	}
	n := len(o.pendL)
	cols := make([]core.Column, len(o.outFields))
	nLeft := len(o.leftFields)
	tmp := make([]cell, n)
	for ci := 0; ci < nLeft; ci++ {
		for r := range o.pendL {
			tmp[r] = o.pendL[r][ci]
		}
		cols[ci] = cellsToColumn(tmp)
	}
	for j, idx := range o.rightOut {
		for r := range o.pendR {
			tmp[r] = o.pendR[r][idx]
		}
		cols[nLeft+j] = cellsToColumn(tmp)
	}
	return core.Record{Chunk: &core.Batch{Schema: core.Schema{Fields: o.outFields}, Len: n, Cols: cols}}
}

// computeOutFields builds the output schema: all left fields, then right fields except
// the redundant right key; a right name colliding with a left name is suffixed "_r".
func (o *sjoinOp) computeOutFields() {
	leftNames := make(map[string]bool, len(o.leftFields))
	for _, f := range o.leftFields {
		leftNames[f.Name] = true
	}
	out := append([]core.Field(nil), o.leftFields...)
	for idx, f := range o.rightFields {
		if f.Name == o.keyField {
			continue // redundant — equals the left key
		}
		name := f.Name
		if leftNames[name] {
			name += "_r"
		}
		out = append(out, core.Field{Name: name, Type: f.Type})
		o.rightOut = append(o.rightOut, idx)
	}
	o.outFields = out
	o.outReady = true
}

// rowCells copies row i of b into a typed cell slice (parallel to b.Cols).
func rowCells(b *core.Batch, i int) []cell {
	cells := make([]cell, len(b.Cols))
	for ci := range b.Cols {
		c := &b.Cols[ci]
		cl := cell{kind: c.Kind}
		switch c.Kind {
		case core.KindInt64:
			cl.i = c.I64[i]
		case core.KindFloat64:
			cl.f = c.F64[i]
		case core.KindString:
			cl.s = c.Str[i]
		case core.KindBool:
			cl.b = c.B[i]
		}
		if c.Null != nil && c.Null[i] {
			cl.null = true
		}
		cells[ci] = cl
	}
	return cells
}

// cellsToColumn rebuilds a typed column (with null mask if any) from one cell per row.
func cellsToColumn(cells []cell) core.Column {
	n := len(cells)
	col := core.Column{Kind: cells[0].kind}
	var null []bool
	mark := func(i int) {
		if null == nil {
			null = make([]bool, n)
		}
		null[i] = true
	}
	switch col.Kind {
	case core.KindFloat64:
		v := make([]float64, n)
		for i, c := range cells {
			v[i] = c.f
			if c.null {
				mark(i)
			}
		}
		col.F64 = v
	case core.KindString:
		v := make([]string, n)
		for i, c := range cells {
			v[i] = c.s
			if c.null {
				mark(i)
			}
		}
		col.Str = v
	case core.KindBool:
		v := make([]bool, n)
		for i, c := range cells {
			v[i] = c.b
			if c.null {
				mark(i)
			}
		}
		col.B = v
	default: // Int64
		v := make([]int64, n)
		for i, c := range cells {
			v[i] = c.i
			if c.null {
				mark(i)
			}
		}
		col.I64 = v
	}
	col.Null = null
	return col
}

func absInt64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
