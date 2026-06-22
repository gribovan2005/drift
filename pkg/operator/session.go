package operator

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
)

// SessionWindow groups records per key into sessions of activity separated by
// gaps of inactivity. A record extends a session if its EventTime is within gap
// of the session's [min, max] span; otherwise it opens a new session. A session
// fires once the watermark (max EventTime seen) reaches sessionMax+gap — meaning
// an event arrived at least gap beyond the session's last event, so nothing more
// can extend it.
//
// Sessions are keyed by keyFn; for a single global session return a constant key.
// The watermark is computed internally from observed EventTimes (single-process
// model), so the DAG executor needs no changes.
type SessionWindow struct {
	gap   time.Duration
	keyFn KeyFunc
	fn    AggregateFunc

	sessions    map[string][]*sess
	maxSeen     time.Time
	firedUpTo   time.Time
	lateDropped int64

	schema core.Schema
}

// sess is one open session: the buffered records and the event-time span.
type sess struct {
	records []core.Record
	min     time.Time
	max     time.Time
}

// NewSessionWindow creates a SessionWindow. gap must be ≥ 1ns and keyFn non-nil.
func NewSessionWindow(gap time.Duration, keyFn KeyFunc, fn AggregateFunc) (*SessionWindow, error) {
	if gap < time.Nanosecond {
		return nil, fmt.Errorf("SessionWindow: gap must be ≥ 1ns, got %s", gap)
	}
	if keyFn == nil {
		return nil, fmt.Errorf("SessionWindow: keyFn must not be nil")
	}
	return &SessionWindow{
		gap:      gap,
		keyFn:    keyFn,
		fn:       fn,
		sessions: make(map[string][]*sess),
	}, nil
}

// Watermark returns the current watermark (max EventTime seen), or the zero time
// if no records have been observed.
func (w *SessionWindow) Watermark() time.Time { return w.maxSeen }

// LateDropped returns the number of records dropped for being too late.
func (w *SessionWindow) LateDropped() int64 { return w.lateDropped }

func (w *SessionWindow) Process(in []core.Record) ([]core.Record, error) {
	for _, r := range in {
		if r.EventTime.After(w.maxSeen) {
			w.maxSeen = r.EventTime
		}
	}

	for _, r := range in {
		w.insert(r)
	}

	out, err := w.fireClosed(w.maxSeen)
	if err != nil {
		return nil, err
	}
	if w.maxSeen.After(w.firedUpTo) {
		w.firedUpTo = w.maxSeen
	}
	return out, nil
}

// insert places r into an existing session (extending/merging) or opens a new
// one. Records too late to ever fire are dropped and counted.
func (w *SessionWindow) insert(r core.Record) {
	k := w.keyFn(r)
	et := r.EventTime
	list := w.sessions[k]

	// Try to extend an existing session: et within [min-gap, max+gap].
	for _, s := range list {
		if !et.Before(s.min.Add(-w.gap)) && !et.After(s.max.Add(w.gap)) {
			s.records = append(s.records, r)
			if et.Before(s.min) {
				s.min = et
			}
			if et.After(s.max) {
				s.max = et
			}
			w.sessions[k] = w.mergeAdjacent(list)
			return
		}
	}

	// New session. Late if it could never fire: its own session end is already
	// past the last fire watermark.
	if !w.firedUpTo.IsZero() && !et.Add(w.gap).After(w.firedUpTo) {
		w.lateDropped++
		return
	}
	s := &sess{records: []core.Record{r}, min: et, max: et}
	w.sessions[k] = w.mergeAdjacent(append(list, s))
}

// mergeAdjacent sorts sessions by start and merges any whose spans come within
// gap of each other, returning the compacted slice.
func (w *SessionWindow) mergeAdjacent(list []*sess) []*sess {
	if len(list) < 2 {
		return list
	}
	sort.Slice(list, func(i, j int) bool { return list[i].min.Before(list[j].min) })

	merged := list[:1]
	for _, s := range list[1:] {
		last := merged[len(merged)-1]
		// Overlap/adjacency within gap: next starts no later than last.max+gap.
		if !s.min.After(last.max.Add(w.gap)) {
			last.records = append(last.records, s.records...)
			if s.max.After(last.max) {
				last.max = s.max
			}
			if s.min.Before(last.min) {
				last.min = s.min
			}
		} else {
			merged = append(merged, s)
		}
	}
	return merged
}

// fireClosed aggregates and removes every session whose max+gap ≤ wm, across all
// keys, in ascending (start, key) order.
func (w *SessionWindow) fireClosed(wm time.Time) ([]core.Record, error) {
	type ready struct {
		key string
		s   *sess
	}
	var rs []ready
	for k, list := range w.sessions {
		kept := list[:0]
		for _, s := range list {
			if !s.max.Add(w.gap).After(wm) { // max+gap ≤ wm → closed
				rs = append(rs, ready{key: k, s: s})
			} else {
				kept = append(kept, s)
			}
		}
		if len(kept) == 0 {
			delete(w.sessions, k)
		} else {
			w.sessions[k] = kept
		}
	}
	if len(rs) == 0 {
		return nil, nil
	}
	sort.Slice(rs, func(i, j int) bool {
		if rs[i].s.min.Equal(rs[j].s.min) {
			return rs[i].key < rs[j].key
		}
		return rs[i].s.min.Before(rs[j].s.min)
	})

	out := make([]core.Record, 0, len(rs))
	for _, r := range rs {
		agg, err := w.fn(r.s.records)
		if err != nil {
			return nil, err
		}
		out = append(out, withParents(agg, r.s.records))
	}
	return out, nil
}

// Flush fires all remaining open sessions in ascending start order. Called by
// the pipeline after the upstream channel closes.
func (w *SessionWindow) Flush() ([]core.Record, error) {
	type ready struct {
		key string
		s   *sess
	}
	var rs []ready
	for k, list := range w.sessions {
		for _, s := range list {
			rs = append(rs, ready{key: k, s: s})
		}
		delete(w.sessions, k)
	}
	if len(rs) == 0 {
		return nil, nil
	}
	sort.Slice(rs, func(i, j int) bool {
		if rs[i].s.min.Equal(rs[j].s.min) {
			return rs[i].key < rs[j].key
		}
		return rs[i].s.min.Before(rs[j].s.min)
	})

	out := make([]core.Record, 0, len(rs))
	for _, r := range rs {
		agg, err := w.fn(r.s.records)
		if err != nil {
			return nil, err
		}
		out = append(out, withParents(agg, r.s.records))
	}
	return out, nil
}

func (w *SessionWindow) OnSchemaChange(s core.Schema) { w.schema = s }

// ── snapshot ──────────────────────────────────────────────────────────────

type sessJSON struct {
	Records []core.Record `json:"records"`
	Min     time.Time     `json:"min"`
	Max     time.Time     `json:"max"`
}

type sessionState struct {
	Sessions    map[string][]sessJSON `json:"sessions"`
	MaxSeen     time.Time             `json:"max_seen"`
	FiredUpTo   time.Time             `json:"fired_up_to"`
	LateDropped int64                 `json:"late_dropped"`
}

// Snapshot serialises all open sessions and watermark state.
func (w *SessionWindow) Snapshot() ([]byte, error) {
	st := sessionState{
		Sessions:    make(map[string][]sessJSON, len(w.sessions)),
		MaxSeen:     w.maxSeen,
		FiredUpTo:   w.firedUpTo,
		LateDropped: w.lateDropped,
	}
	for k, list := range w.sessions {
		js := make([]sessJSON, len(list))
		for i, s := range list {
			js[i] = sessJSON{Records: s.records, Min: s.min, Max: s.max}
		}
		st.Sessions[k] = js
	}
	return json.Marshal(st)
}

// Restore loads a previously saved state.
func (w *SessionWindow) Restore(data []byte) error {
	var st sessionState
	if err := json.Unmarshal(data, &st); err != nil {
		return fmt.Errorf("SessionWindow restore: %w", err)
	}
	w.sessions = make(map[string][]*sess, len(st.Sessions))
	for k, js := range st.Sessions {
		list := make([]*sess, len(js))
		for i, s := range js {
			list[i] = &sess{records: s.Records, min: s.Min, max: s.Max}
		}
		w.sessions[k] = list
	}
	w.maxSeen = st.MaxSeen
	w.firedUpTo = st.FiredUpTo
	w.lateDropped = st.LateDropped
	return nil
}
