package operator

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/andrejgribov/drift/pkg/core"
)

// KeyedAggregateFunc reduces one key's window of records to a single output. The
// key is passed so the aggregate can label its output (e.g. group-by key).
type KeyedAggregateFunc func(key string, window []core.Record) (core.Record, error)

// KeyedCountWindow is a group-by aggregation over fixed-count windows per key:
// records are bucketed by keyFn, and once a key accumulates `size` records the
// aggregate fires for that key and its buffer resets. This is the building block
// for Nexmark group-by queries (count/sum/min/max/etc per key). It is a count
// window (not time-based); event-time keyed windows can build on the same shape.
//
// Implements core.Flusher (emit partial windows at shutdown) and
// core.Snapshottable (checkpoint per-key buffers). It is keyed, so it composes
// with pipeline.Parallel (shard by the same key).
type KeyedCountWindow struct {
	keyFn KeyFunc
	size  int
	fn    KeyedAggregateFunc

	bufs   map[string][]core.Record
	schema core.Schema
}

// NewKeyedCountWindow creates a KeyedCountWindow. size must be ≥ 1, keyFn and fn
// non-nil.
func NewKeyedCountWindow(keyFn KeyFunc, size int, fn KeyedAggregateFunc) (*KeyedCountWindow, error) {
	if size < 1 {
		return nil, fmt.Errorf("KeyedCountWindow: size must be ≥ 1, got %d", size)
	}
	if keyFn == nil || fn == nil {
		return nil, fmt.Errorf("KeyedCountWindow: keyFn and fn must not be nil")
	}
	return &KeyedCountWindow{keyFn: keyFn, size: size, fn: fn, bufs: make(map[string][]core.Record)}, nil
}

func (w *KeyedCountWindow) Process(in []core.Record) ([]core.Record, error) {
	var out []core.Record
	for _, r := range in {
		k := w.keyFn(r)
		buf := append(w.bufs[k], r)
		if len(buf) >= w.size {
			agg, err := w.fn(k, buf)
			if err != nil {
				return nil, err
			}
			out = append(out, withParents(agg, buf))
			delete(w.bufs, k)
		} else {
			w.bufs[k] = buf
		}
	}
	return out, nil
}

// Flush emits the partial window for every key still buffered, in key order.
func (w *KeyedCountWindow) Flush() ([]core.Record, error) {
	keys := make([]string, 0, len(w.bufs))
	for k := range w.bufs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var out []core.Record
	for _, k := range keys {
		buf := w.bufs[k]
		agg, err := w.fn(k, buf)
		if err != nil {
			return nil, err
		}
		out = append(out, withParents(agg, buf))
	}
	w.bufs = make(map[string][]core.Record)
	return out, nil
}

func (w *KeyedCountWindow) OnSchemaChange(s core.Schema) { w.schema = s }

// ── snapshot ────────────────────────────────────────────────────────────────

func (w *KeyedCountWindow) Snapshot() ([]byte, error) {
	return json.Marshal(w.bufs)
}

func (w *KeyedCountWindow) Restore(data []byte) error {
	var bufs map[string][]core.Record
	if err := json.Unmarshal(data, &bufs); err != nil {
		return fmt.Errorf("KeyedCountWindow restore: %w", err)
	}
	if bufs == nil {
		bufs = make(map[string][]core.Record)
	}
	w.bufs = bufs
	return nil
}

// ── common aggregates ───────────────────────────────────────────────────────

// CountAgg emits {key: <keyField>, count: N} for each window.
func CountAgg(keyField string) KeyedAggregateFunc {
	return func(key string, window []core.Record) (core.Record, error) {
		return core.Record{Payload: map[string]any{keyField: key, "count": len(window)}}, nil
	}
}

// MaxAgg emits {key, max} — the maximum of valueField across the window.
func MaxAgg(keyField, valueField string) KeyedAggregateFunc {
	return func(key string, window []core.Record) (core.Record, error) {
		var max float64
		first := true
		for _, r := range window {
			if v, ok := aggFloat(r.Payload[valueField]); ok && (first || v > max) {
				max, first = v, false
			}
		}
		return core.Record{Payload: map[string]any{keyField: key, "max": max}}, nil
	}
}

// AvgAgg emits {key, avg} — the mean of valueField across the window.
func AvgAgg(keyField, valueField string) KeyedAggregateFunc {
	return func(key string, window []core.Record) (core.Record, error) {
		var sum float64
		var n int
		for _, r := range window {
			if v, ok := aggFloat(r.Payload[valueField]); ok {
				sum += v
				n++
			}
		}
		avg := 0.0
		if n > 0 {
			avg = sum / float64(n)
		}
		return core.Record{Payload: map[string]any{keyField: key, "avg": avg}}, nil
	}
}

// aggFloat coerces a numeric payload value to float64.
func aggFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}
