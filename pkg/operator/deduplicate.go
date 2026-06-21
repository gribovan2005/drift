package operator

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/andrejgribov/drift/pkg/core"
)

// KeyFunc extracts a string deduplication key from a record.
type KeyFunc func(core.Record) string

// Deduplicate filters records whose key was seen within the last window
// duration. window=0 disables deduplication — all records pass through.
// Implements core.Snapshottable for checkpoint support.
type Deduplicate struct {
	keyFn  KeyFunc
	window time.Duration
	seen   map[string]time.Time // only accessed from Process goroutine
	nowFn  func() time.Time     // injectable for tests; defaults to time.Now
	schema core.Schema
}

// NewDeduplicate creates a Deduplicate operator.
func NewDeduplicate(keyFn KeyFunc, window time.Duration) *Deduplicate {
	return &Deduplicate{
		keyFn:  keyFn,
		window: window,
		seen:   make(map[string]time.Time),
		nowFn:  time.Now,
	}
}

func (d *Deduplicate) Process(in []core.Record) ([]core.Record, error) {
	// window=0 disables deduplication entirely.
	if d.window == 0 {
		return in, nil
	}

	now := d.nowFn()

	// Lazy eviction of expired keys.
	for k, t := range d.seen {
		if now.Sub(t) >= d.window {
			delete(d.seen, k)
		}
	}

	out := in[:0]
	for _, r := range in {
		key := d.keyFn(r)
		if _, dup := d.seen[key]; dup {
			continue
		}
		d.seen[key] = now
		out = append(out, r)
	}
	return out, nil
}

func (d *Deduplicate) OnSchemaChange(s core.Schema) { d.schema = s }

type dedupState struct {
	Seen map[string]time.Time `json:"seen"`
}

// Snapshot serialises the seen map. Called after all stage goroutines exit.
func (d *Deduplicate) Snapshot() ([]byte, error) {
	return json.Marshal(dedupState{Seen: d.seen})
}

// Restore deserialises a previously saved seen map, pruning expired entries.
func (d *Deduplicate) Restore(data []byte) error {
	var s dedupState
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("Deduplicate restore: %w", err)
	}
	now := d.nowFn()
	for k, t := range s.Seen {
		if now.Sub(t) >= d.window {
			delete(s.Seen, k)
		}
	}
	d.seen = s.Seen
	return nil
}
