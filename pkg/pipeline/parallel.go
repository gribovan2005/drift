package pipeline

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/andrejgribov/drift/pkg/core"
)

// Parallel runs k instances of an operator (one per shard) concurrently, routing
// each record to a shard so a single stage uses multiple cores (intra-stage data
// parallelism). With key == nil records are spread round-robin — correct for
// stateless operators. With a key func, records sharing a key always land on the
// same shard, so keyed stateful operators (dedup, session) stay correct. The
// wrapper preserves the inner operators' Flusher / Snapshottable interfaces, so
// windowing and checkpointing keep working. ops must be non-empty.
func Parallel(ops []core.Operator, key func(core.Record) string) core.Operator {
	base := &parallelOp{ops: ops, key: key}
	_, isFlusher := ops[0].(core.Flusher)
	_, isSnap := ops[0].(core.Snapshottable)
	switch {
	case isFlusher && isSnap:
		return &parFlushSnap{base}
	case isFlusher:
		return &parFlush{base}
	case isSnap:
		return &parSnap{base}
	default:
		return base
	}
}

type parallelOp struct {
	ops []core.Operator
	key func(core.Record) string // nil → round-robin
}

func (p *parallelOp) shardOf(r core.Record, idx int) int {
	if p.key == nil {
		return idx % len(p.ops)
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(p.key(r)))
	return int(h.Sum32()) % len(p.ops)
}

func (p *parallelOp) Process(in []core.Record) ([]core.Record, error) {
	k := len(p.ops)
	buckets := make([][]core.Record, k)
	for i, r := range in {
		s := p.shardOf(r, i)
		buckets[s] = append(buckets[s], r)
	}
	outs := make([][]core.Record, k)
	errs := make([]error, k)
	var wg sync.WaitGroup
	for i := range p.ops {
		if len(buckets[i]) == 0 {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			outs[i], errs[i] = p.ops[i].Process(buckets[i])
		}(i)
	}
	wg.Wait()
	return gather(outs, errs)
}

func (p *parallelOp) OnSchemaChange(s core.Schema) {
	for _, op := range p.ops {
		op.OnSchemaChange(s)
	}
}

func (p *parallelOp) flush() ([]core.Record, error) {
	outs := make([][]core.Record, len(p.ops))
	errs := make([]error, len(p.ops))
	var wg sync.WaitGroup
	for i, op := range p.ops {
		f, ok := op.(core.Flusher)
		if !ok {
			continue
		}
		wg.Add(1)
		go func(i int, f core.Flusher) {
			defer wg.Done()
			outs[i], errs[i] = f.Flush()
		}(i, f)
	}
	wg.Wait()
	return gather(outs, errs)
}

func (p *parallelOp) snapshot() ([]byte, error) {
	shards := make([]json.RawMessage, len(p.ops))
	for i, op := range p.ops {
		s, ok := op.(core.Snapshottable)
		if !ok {
			continue
		}
		b, err := s.Snapshot()
		if err != nil {
			return nil, fmt.Errorf("parallel snapshot shard %d: %w", i, err)
		}
		shards[i] = b
	}
	return json.Marshal(shards)
}

func (p *parallelOp) restore(data []byte) error {
	var shards []json.RawMessage
	if err := json.Unmarshal(data, &shards); err != nil {
		return fmt.Errorf("parallel restore: %w", err)
	}
	if len(shards) != len(p.ops) {
		return fmt.Errorf("parallel restore: shard count %d != %d", len(shards), len(p.ops))
	}
	for i, op := range p.ops {
		s, ok := op.(core.Snapshottable)
		if !ok || len(shards[i]) == 0 {
			continue
		}
		if err := s.Restore(shards[i]); err != nil {
			return fmt.Errorf("parallel restore shard %d: %w", i, err)
		}
	}
	return nil
}

// gather concatenates shard outputs in shard order, returning the first error.
func gather(outs [][]core.Record, errs []error) ([]core.Record, error) {
	var out []core.Record
	for i := range outs {
		if errs[i] != nil {
			return nil, errs[i]
		}
		out = append(out, outs[i]...)
	}
	return out, nil
}

// ── interface-preserving variants ───────────────────────────────────────────

type parFlush struct{ *parallelOp }

func (p *parFlush) Flush() ([]core.Record, error) { return p.flush() }

type parSnap struct{ *parallelOp }

func (p *parSnap) Snapshot() ([]byte, error) { return p.snapshot() }
func (p *parSnap) Restore(b []byte) error    { return p.restore(b) }

type parFlushSnap struct{ *parallelOp }

func (p *parFlushSnap) Flush() ([]core.Record, error) { return p.flush() }
func (p *parFlushSnap) Snapshot() ([]byte, error)     { return p.snapshot() }
func (p *parFlushSnap) Restore(b []byte) error        { return p.restore(b) }
