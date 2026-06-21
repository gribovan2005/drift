package operator

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/andrejgribov/drift/pkg/core"
)

// ── TimestampAssigner ─────────────────────────────────────────────────────

// TimestampFunc extracts an event time from a record (e.g. a payload field).
type TimestampFunc func(core.Record) time.Time

// TimestampAssigner populates Record.EventTime from a user-supplied function.
// Place it before any event-time window so downstream operators have a
// populated EventTime. It is stateless and 1-to-1 (never drops records).
type TimestampAssigner struct {
	fn TimestampFunc
}

// NewTimestampAssigner creates a TimestampAssigner from fn.
func NewTimestampAssigner(fn TimestampFunc) *TimestampAssigner {
	return &TimestampAssigner{fn: fn}
}

func (t *TimestampAssigner) Process(in []core.Record) ([]core.Record, error) {
	if len(in) == 0 {
		return in, nil
	}
	out := make([]core.Record, len(in))
	for i, r := range in {
		r.EventTime = t.fn(r)
		out[i] = r
	}
	return out, nil
}

func (t *TimestampAssigner) OnSchemaChange(core.Schema) {}

// ── EventTimeWindow ───────────────────────────────────────────────────────

// EventTimeWindow is a tumbling window keyed on event time. A record falls into
// the window [start, start+size) where start is its EventTime truncated to a
// size boundary. A window fires once the watermark (maxEventTimeSeen −
// allowedLateness) reaches the window end. Records whose window has already
// fired are late: they are dropped and counted.
//
// The watermark is computed internally from observed EventTimes — there is no
// separate watermark event threaded through the pipeline (single-process model).
type EventTimeWindow struct {
	size     time.Duration
	lateness time.Duration
	fn       AggregateFunc

	windows     map[int64][]core.Record // key: window start (unix nano)
	maxSeen     time.Time               // max EventTime observed
	firedUpTo   time.Time               // watermark at which windows were last fired
	lateDropped int64

	schema core.Schema
}

// NewEventTimeWindow creates an EventTimeWindow. size must be ≥ 1ns and
// allowedLateness must be ≥ 0.
func NewEventTimeWindow(size, allowedLateness time.Duration, fn AggregateFunc) (*EventTimeWindow, error) {
	if size < time.Nanosecond {
		return nil, fmt.Errorf("EventTimeWindow: size must be ≥ 1ns, got %s", size)
	}
	if allowedLateness < 0 {
		return nil, fmt.Errorf("EventTimeWindow: allowedLateness must be ≥ 0, got %s", allowedLateness)
	}
	return &EventTimeWindow{
		size:     size,
		lateness: allowedLateness,
		fn:       fn,
		windows:  make(map[int64][]core.Record),
	}, nil
}

// windowStart aligns t down to a size boundary (from the zero time).
func (w *EventTimeWindow) windowStart(t time.Time) time.Time {
	return t.Truncate(w.size)
}

// Watermark returns the current watermark: maxEventTimeSeen − allowedLateness.
// Returns the zero time if no records have been seen yet.
func (w *EventTimeWindow) Watermark() time.Time {
	if w.maxSeen.IsZero() {
		return time.Time{}
	}
	return w.maxSeen.Add(-w.lateness)
}

// LateDropped returns the number of records dropped for being too late.
func (w *EventTimeWindow) LateDropped() int64 { return w.lateDropped }

func (w *EventTimeWindow) Process(in []core.Record) ([]core.Record, error) {
	// Advance maxSeen across the whole batch first so the watermark reflects
	// the most recent event before we decide which records are late.
	for _, r := range in {
		if r.EventTime.After(w.maxSeen) {
			w.maxSeen = r.EventTime
		}
	}
	wm := w.Watermark()

	for _, r := range in {
		start := w.windowStart(r.EventTime)
		end := start.Add(w.size)
		// Late: this window was already fired in a previous batch
		// (end ≤ firedUpTo). Records whose window closes *within* this batch
		// are still accepted — they are aggregated before fireClosed runs.
		if !w.firedUpTo.IsZero() && !end.After(w.firedUpTo) {
			w.lateDropped++
			continue
		}
		key := start.UnixNano()
		w.windows[key] = append(w.windows[key], r)
	}

	out, err := w.fireClosed(wm)
	if err != nil {
		return nil, err
	}
	if wm.After(w.firedUpTo) {
		w.firedUpTo = wm
	}
	return out, nil
}

// fireClosed aggregates and removes every window whose end ≤ wm, in ascending
// start order.
func (w *EventTimeWindow) fireClosed(wm time.Time) ([]core.Record, error) {
	var ready []int64
	for key := range w.windows {
		start := time.Unix(0, key)
		if !start.Add(w.size).After(wm) { // end ≤ wm
			ready = append(ready, key)
		}
	}
	if len(ready) == 0 {
		return nil, nil
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i] < ready[j] })

	out := make([]core.Record, 0, len(ready))
	for _, key := range ready {
		agg, err := w.fn(w.windows[key])
		if err != nil {
			return nil, err
		}
		out = append(out, withParents(agg, w.windows[key]))
		delete(w.windows, key)
	}
	return out, nil
}

// Flush advances the watermark to +∞ and emits all remaining windows in
// ascending start order. Called by the pipeline after the upstream closes.
func (w *EventTimeWindow) Flush() ([]core.Record, error) {
	if len(w.windows) == 0 {
		return nil, nil
	}
	keys := make([]int64, 0, len(w.windows))
	for key := range w.windows {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	out := make([]core.Record, 0, len(keys))
	for _, key := range keys {
		agg, err := w.fn(w.windows[key])
		if err != nil {
			return nil, err
		}
		out = append(out, withParents(agg, w.windows[key]))
		delete(w.windows, key)
	}
	return out, nil
}

func (w *EventTimeWindow) OnSchemaChange(s core.Schema) { w.schema = s }

type eventTimeState struct {
	Windows     map[int64][]core.Record `json:"windows"`
	MaxSeen     time.Time               `json:"max_seen"`
	FiredUpTo   time.Time               `json:"fired_up_to"`
	LateDropped int64                   `json:"late_dropped"`
}

// Snapshot serialises open windows, the max event time, the fire watermark,
// and the late count.
func (w *EventTimeWindow) Snapshot() ([]byte, error) {
	return json.Marshal(eventTimeState{
		Windows:     w.windows,
		MaxSeen:     w.maxSeen,
		FiredUpTo:   w.firedUpTo,
		LateDropped: w.lateDropped,
	})
}

// Restore loads a previously saved state.
func (w *EventTimeWindow) Restore(data []byte) error {
	var s eventTimeState
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("EventTimeWindow restore: %w", err)
	}
	if s.Windows == nil {
		s.Windows = make(map[int64][]core.Record)
	}
	w.windows = s.Windows
	w.maxSeen = s.MaxSeen
	w.firedUpTo = s.FiredUpTo
	w.lateDropped = s.LateDropped
	return nil
}
