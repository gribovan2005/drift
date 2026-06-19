package operator

import (
	"encoding/json"
	"fmt"

	"github.com/andrejgribov/drift/pkg/core"
)

// AggregateFunc reduces a window of records into a single output record.
type AggregateFunc func(window []core.Record) (core.Record, error)

// ── TumblingWindow ────────────────────────────────────────────────────────

// TumblingWindow collects exactly size records then emits one aggregated
// record. Non-overlapping: each record belongs to exactly one window.
// On pipeline shutdown, any partial window is emitted via Flush().
type TumblingWindow struct {
	size   int
	fn     AggregateFunc
	buf    []core.Record
	schema core.Schema
}

// NewTumblingWindow creates a TumblingWindow. size must be ≥ 1.
func NewTumblingWindow(size int, fn AggregateFunc) (*TumblingWindow, error) {
	if size < 1 {
		return nil, fmt.Errorf("TumblingWindow: size must be ≥ 1, got %d", size)
	}
	return &TumblingWindow{size: size, fn: fn, buf: make([]core.Record, 0, size)}, nil
}

func (w *TumblingWindow) Process(in []core.Record) ([]core.Record, error) {
	var out []core.Record
	for _, r := range in {
		w.buf = append(w.buf, r)
		if len(w.buf) >= w.size {
			agg, err := w.fn(w.buf)
			if err != nil {
				return nil, err
			}
			out = append(out, agg)
			w.buf = w.buf[:0]
		}
	}
	return out, nil
}

// Flush emits a partial window if any records remain in the buffer.
// Called by the pipeline after the upstream channel closes.
func (w *TumblingWindow) Flush() ([]core.Record, error) {
	if len(w.buf) == 0 {
		return nil, nil
	}
	agg, err := w.fn(w.buf)
	if err != nil {
		return nil, err
	}
	w.buf = w.buf[:0]
	return []core.Record{agg}, nil
}

func (w *TumblingWindow) OnSchemaChange(s core.Schema) { w.schema = s }

type tumblingState struct {
	Buf []core.Record `json:"buf"`
}

// Snapshot serialises the internal buffer. Called by the pipeline after all
// stage goroutines have exited — no concurrent Process calls at this point.
func (w *TumblingWindow) Snapshot() ([]byte, error) {
	return json.Marshal(tumblingState{Buf: w.buf})
}

// Restore deserialises a previously saved buffer. Called before the first
// Process call on pipeline restart.
func (w *TumblingWindow) Restore(data []byte) error {
	var s tumblingState
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("TumblingWindow restore: %w", err)
	}
	w.buf = s.Buf
	return nil
}

// ── SlidingWindow ─────────────────────────────────────────────────────────

// SlidingWindow emits one aggregate after every step records, using the last
// size records as the window content. Windows overlap when step < size.
// On pipeline shutdown, a final window is emitted via Flush() if there are
// unseen records since the last emission.
type SlidingWindow struct {
	size   int
	step   int
	fn     AggregateFunc
	buf    []core.Record // sliding buffer: last min(total, size) records
	count  int           // records accumulated since last emission
	schema core.Schema
}

// NewSlidingWindow creates a SlidingWindow. size must be ≥ step ≥ 1.
func NewSlidingWindow(size, step int, fn AggregateFunc) (*SlidingWindow, error) {
	if step < 1 {
		return nil, fmt.Errorf("SlidingWindow: step must be ≥ 1, got %d", step)
	}
	if size < step {
		return nil, fmt.Errorf("SlidingWindow: size (%d) must be ≥ step (%d)", size, step)
	}
	return &SlidingWindow{size: size, step: step, fn: fn, buf: make([]core.Record, 0, size)}, nil
}

func (w *SlidingWindow) Process(in []core.Record) ([]core.Record, error) {
	var out []core.Record
	for _, r := range in {
		w.buf = append(w.buf, r)
		if len(w.buf) > w.size {
			// Trim oldest record from the front.
			// Copy forward to avoid holding the underlying array indefinitely.
			copy(w.buf, w.buf[1:])
			w.buf = w.buf[:len(w.buf)-1]
		}
		w.count++
		if w.count >= w.step {
			agg, err := w.fn(w.buf)
			if err != nil {
				return nil, err
			}
			out = append(out, agg)
			w.count = 0
		}
	}
	return out, nil
}

// Flush emits a final window if records have accumulated since the last
// emission (i.e. the stream ended mid-step).
func (w *SlidingWindow) Flush() ([]core.Record, error) {
	if w.count == 0 || len(w.buf) == 0 {
		return nil, nil
	}
	agg, err := w.fn(w.buf)
	if err != nil {
		return nil, err
	}
	w.count = 0
	return []core.Record{agg}, nil
}

func (w *SlidingWindow) OnSchemaChange(s core.Schema) { w.schema = s }

type slidingState struct {
	Buf   []core.Record `json:"buf"`
	Count int           `json:"count"`
}

// Snapshot saves the sliding buffer and step counter.
func (w *SlidingWindow) Snapshot() ([]byte, error) {
	return json.Marshal(slidingState{Buf: w.buf, Count: w.count})
}

// Restore loads a previously saved buffer and step counter.
func (w *SlidingWindow) Restore(data []byte) error {
	var s slidingState
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("SlidingWindow restore: %w", err)
	}
	w.buf = s.Buf
	w.count = s.Count
	return nil
}
