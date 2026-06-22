package operator

import (
	"encoding/json"
	"fmt"

	"github.com/gribovan2005/drift/pkg/core"
)

// JoinFunc combines a matched left+right pair into one output record.
type JoinFunc func(left, right core.Record) (core.Record, error)

// Join is a streaming hash equi-join over a single mixed stream. Drift's Nexmark
// source interleaves event types (person/auction/bid) in one stream, so a join
// reads both sides from that stream: records are dispatched by SchemaID, buffered
// per join key (bounded to the last maxPerKey per key to keep state finite — a
// count-window proxy for a time-bounded join), and each arrival is matched
// against the opposite side's buffer. Inner join semantics.
//
// Implements core.Snapshottable. It is not a Flusher: matches are emitted
// eagerly, nothing is buffered for output.
type Join struct {
	leftType, rightType string
	leftKey, rightKey   KeyFunc
	fn                  JoinFunc
	maxPerKey           int

	left, right map[string][]core.Record
	schema      core.Schema
}

// NewJoin creates a Join. leftType/rightType are the SchemaIDs of the two sides,
// maxPerKey (≥1) bounds buffered records per key per side.
func NewJoin(leftType string, leftKey KeyFunc, rightType string, rightKey KeyFunc, maxPerKey int, fn JoinFunc) (*Join, error) {
	if leftKey == nil || rightKey == nil || fn == nil {
		return nil, fmt.Errorf("Join: leftKey, rightKey and fn must not be nil")
	}
	if maxPerKey < 1 {
		return nil, fmt.Errorf("Join: maxPerKey must be ≥ 1, got %d", maxPerKey)
	}
	return &Join{
		leftType: leftType, rightType: rightType,
		leftKey: leftKey, rightKey: rightKey, fn: fn, maxPerKey: maxPerKey,
		left:  make(map[string][]core.Record),
		right: make(map[string][]core.Record),
	}, nil
}

func (j *Join) Process(in []core.Record) ([]core.Record, error) {
	var out []core.Record
	for _, r := range in {
		switch r.SchemaID {
		case j.leftType:
			k := j.leftKey(r)
			for _, m := range j.right[k] {
				res, err := j.fn(r, m)
				if err != nil {
					return nil, err
				}
				out = append(out, withParents(res, []core.Record{r, m}))
			}
			j.left[k] = push(j.left[k], r, j.maxPerKey)
		case j.rightType:
			k := j.rightKey(r)
			for _, m := range j.left[k] {
				res, err := j.fn(m, r)
				if err != nil {
					return nil, err
				}
				out = append(out, withParents(res, []core.Record{m, r}))
			}
			j.right[k] = push(j.right[k], r, j.maxPerKey)
		default:
			// Event type not part of this join — drop it.
		}
	}
	return out, nil
}

func (j *Join) OnSchemaChange(s core.Schema) { j.schema = s }

// push appends r to buf, keeping at most maxPerKey (drops the oldest).
func push(buf []core.Record, r core.Record, max int) []core.Record {
	buf = append(buf, r)
	if len(buf) > max {
		buf = append(buf[:0:0], buf[len(buf)-max:]...)
	}
	return buf
}

// ── snapshot ────────────────────────────────────────────────────────────────

type joinState struct {
	Left  map[string][]core.Record `json:"left"`
	Right map[string][]core.Record `json:"right"`
}

func (j *Join) Snapshot() ([]byte, error) {
	return json.Marshal(joinState{Left: j.left, Right: j.right})
}

func (j *Join) Restore(data []byte) error {
	var s joinState
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("Join restore: %w", err)
	}
	j.left, j.right = s.Left, s.Right
	if j.left == nil {
		j.left = make(map[string][]core.Record)
	}
	if j.right == nil {
		j.right = make(map[string][]core.Record)
	}
	return nil
}
