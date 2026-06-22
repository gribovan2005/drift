package operator

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/gribovan2005/drift/pkg/core"
)

// ValueFunc extracts the ordering value of a record for ranking (higher = better).
type ValueFunc func(core.Record) float64

// TopN keeps the top-n records by value within a fixed-count window, optionally
// per key. With keyFn == nil it ranks globally (one bucket); with a keyFn it
// ranks within each key. When a bucket fills (size records) it emits the top n
// (each tagged with a 1-based "rank") and resets. This powers Nexmark q19 (top
// bids per auction) and q5 (hottest auctions per window).
//
// Implements core.Flusher (emit partial buckets) and core.Snapshottable.
type TopN struct {
	keyFn KeyFunc // nil → global
	byFn  ValueFunc
	n     int
	size  int

	bufs   map[string][]core.Record
	schema core.Schema
}

// NewTopN creates a TopN. n and size must be ≥ 1, byFn non-nil.
func NewTopN(keyFn KeyFunc, byFn ValueFunc, n, size int) (*TopN, error) {
	if n < 1 || size < 1 {
		return nil, fmt.Errorf("TopN: n and size must be ≥ 1 (got n=%d size=%d)", n, size)
	}
	if byFn == nil {
		return nil, fmt.Errorf("TopN: byFn must not be nil")
	}
	return &TopN{keyFn: keyFn, byFn: byFn, n: n, size: size, bufs: make(map[string][]core.Record)}, nil
}

func (t *TopN) key(r core.Record) string {
	if t.keyFn == nil {
		return ""
	}
	return t.keyFn(r)
}

func (t *TopN) Process(in []core.Record) ([]core.Record, error) {
	var out []core.Record
	for _, r := range in {
		k := t.key(r)
		buf := append(t.bufs[k], r)
		if len(buf) >= t.size {
			out = append(out, t.emitTop(buf)...)
			delete(t.bufs, k)
		} else {
			t.bufs[k] = buf
		}
	}
	return out, nil
}

// Flush emits the top-n of every partial bucket, in key order.
func (t *TopN) Flush() ([]core.Record, error) {
	keys := make([]string, 0, len(t.bufs))
	for k := range t.bufs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var out []core.Record
	for _, k := range keys {
		out = append(out, t.emitTop(t.bufs[k])...)
	}
	t.bufs = make(map[string][]core.Record)
	return out, nil
}

// emitTop returns the top-n records of buf by byFn (desc), each tagged with rank.
func (t *TopN) emitTop(buf []core.Record) []core.Record {
	ranked := make([]core.Record, len(buf))
	copy(ranked, buf)
	sort.SliceStable(ranked, func(i, j int) bool { return t.byFn(ranked[i]) > t.byFn(ranked[j]) })

	n := t.n
	if n > len(ranked) {
		n = len(ranked)
	}
	out := make([]core.Record, n)
	for i := 0; i < n; i++ {
		r := withParents(ranked[i], buf)
		r.Payload = clonePayload(r.Payload)
		r.Payload["rank"] = i + 1
		out[i] = r
	}
	return out
}

func (t *TopN) OnSchemaChange(s core.Schema) { t.schema = s }

func (t *TopN) Snapshot() ([]byte, error) { return json.Marshal(t.bufs) }

func (t *TopN) Restore(data []byte) error {
	var bufs map[string][]core.Record
	if err := json.Unmarshal(data, &bufs); err != nil {
		return fmt.Errorf("TopN restore: %w", err)
	}
	if bufs == nil {
		bufs = make(map[string][]core.Record)
	}
	t.bufs = bufs
	return nil
}

// clonePayload makes a shallow copy so tagging rank doesn't mutate a shared map.
func clonePayload(p map[string]any) map[string]any {
	out := make(map[string]any, len(p)+1)
	for k, v := range p {
		out[k] = v
	}
	return out
}
