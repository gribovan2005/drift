package lineage

import "github.com/andrejgribov/drift/pkg/core"

// Wrap returns an operator decorator that records provenance for every record
// flowing through op under the given stage label. The returned value preserves
// op's optional interfaces: if op is a core.Flusher and/or core.Snapshottable,
// the wrapper exposes the same so windowing and checkpointing keep working.
//
// Operators that are not Flushers are invoked per record so each output's parent
// is the exact input it came from. Flushers (aggregating windows) are invoked
// per batch; their outputs are attributed to all inputs in the Process call.
func (t *Tracker) Wrap(stage string, op core.Operator) core.Operator {
	_, isFlusher := op.(core.Flusher)
	_, isSnap := op.(core.Snapshottable)
	base := &tracked{stage: stage, inner: op, t: t, batch: isFlusher}

	switch {
	case isFlusher && isSnap:
		return &trackedFlushSnap{base}
	case isFlusher:
		return &trackedFlusher{base}
	case isSnap:
		return &trackedSnap{base}
	default:
		return base
	}
}

// tracked is the base lineage decorator implementing core.Operator.
type tracked struct {
	stage string
	inner core.Operator
	t     *Tracker
	batch bool // inner is a Flusher → attribute outputs to the whole batch
}

func (w *tracked) Process(in []core.Record) ([]core.Record, error) {
	w.ensureRoots(in)
	if w.batch {
		return w.processBatch(in)
	}
	return w.processPerRecord(in)
}

// ensureRoots assigns IDs and records root nodes for any input lacking an ID
// (i.e. records arriving straight from the source).
func (w *tracked) ensureRoots(in []core.Record) {
	for i := range in {
		if in[i].ID == "" {
			id := w.t.nextID()
			in[i].ID = id
			w.t.add(&Node{
				ID:            id,
				Stage:         sourceStage,
				SchemaID:      in[i].SchemaID,
				SchemaVersion: in[i].SchemaVersion,
			})
		}
	}
}

// processPerRecord invokes the inner operator once per input so each output is
// attributed to the single record it derived from.
func (w *tracked) processPerRecord(in []core.Record) ([]core.Record, error) {
	var out []core.Record
	for _, r := range in {
		res, err := w.inner.Process([]core.Record{r})
		if err != nil {
			return nil, err
		}
		for _, o := range res {
			o.ID = w.t.nextID()
			o.Parents = []string{r.ID}
			w.record(&o)
			out = append(out, o)
		}
	}
	return out, nil
}

// processBatch invokes the inner operator on the whole batch. Aggregating
// operators stamp each output's Parents with the exact records that produced it
// (per window, not per batch); when an operator declares no parents the output
// falls back to the whole input batch.
func (w *tracked) processBatch(in []core.Record) ([]core.Record, error) {
	res, err := w.inner.Process(in)
	if err != nil {
		return nil, err
	}
	batchParents := recordIDs(in)
	out := make([]core.Record, 0, len(res))
	for _, o := range res {
		if o.Parents == nil {
			o.Parents = batchParents
		}
		o.ID = w.t.nextID()
		w.record(&o)
		out = append(out, o)
	}
	return out, nil
}

func (w *tracked) OnSchemaChange(s core.Schema) { w.inner.OnSchemaChange(s) }

// record adds a node for an output record.
func (w *tracked) record(o *core.Record) {
	w.t.add(&Node{
		ID:            o.ID,
		Stage:         w.stage,
		SchemaID:      o.SchemaID,
		SchemaVersion: o.SchemaVersion,
		Parents:       o.Parents,
	})
}

// flush drains the inner Flusher and records the flushed outputs. Aggregating
// operators retain their buffered records, so flushed outputs carry exact
// parents; any operator that declares none yields a parentless output.
func (w *tracked) flush() ([]core.Record, error) {
	res, err := w.inner.(core.Flusher).Flush()
	if err != nil {
		return nil, err
	}
	out := make([]core.Record, 0, len(res))
	for _, o := range res {
		o.ID = w.t.nextID()
		w.record(&o)
		out = append(out, o)
	}
	return out, nil
}

func recordIDs(rs []core.Record) []string {
	ids := make([]string, len(rs))
	for i, r := range rs {
		ids[i] = r.ID
	}
	return ids
}

// ── Interface-preserving variants ──────────────────────────────────────────

type trackedFlusher struct{ *tracked }

func (w *trackedFlusher) Flush() ([]core.Record, error) { return w.flush() }

type trackedSnap struct{ *tracked }

func (w *trackedSnap) Snapshot() ([]byte, error) { return w.inner.(core.Snapshottable).Snapshot() }
func (w *trackedSnap) Restore(b []byte) error    { return w.inner.(core.Snapshottable).Restore(b) }

type trackedFlushSnap struct{ *tracked }

func (w *trackedFlushSnap) Flush() ([]core.Record, error) { return w.flush() }
func (w *trackedFlushSnap) Snapshot() ([]byte, error) {
	return w.inner.(core.Snapshottable).Snapshot()
}
func (w *trackedFlushSnap) Restore(b []byte) error { return w.inner.(core.Snapshottable).Restore(b) }
