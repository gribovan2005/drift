package vector

import (
	"context"
	"sync"

	"github.com/gribovan2005/drift/pkg/core"
)

// Discard drains chunk-records and keeps nothing — the fast-lane load-test sink.
func Discard() core.Sink { return discard{} }

type discard struct{}

func (discard) Write(ctx context.Context, ch <-chan core.Record) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-ch:
			if !ok {
				return nil
			}
		}
	}
}

// Collector keeps every chunk it receives. Read results after Run with Batches()
// or the total surviving row count with Rows().
type Collector struct {
	mu      sync.Mutex
	batches []*core.Batch
}

// Collect returns a fresh columnar Collector sink.
func Collect() *Collector { return &Collector{} }

func (c *Collector) Write(ctx context.Context, ch <-chan core.Record) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r, ok := <-ch:
			if !ok {
				return nil
			}
			if r.Chunk != nil {
				c.mu.Lock()
				c.batches = append(c.batches, r.Chunk)
				c.mu.Unlock()
			}
		}
	}
}

// Batches returns the collected chunks (safe after Run).
func (c *Collector) Batches() []*core.Batch {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*core.Batch, len(c.batches))
	copy(out, c.batches)
	return out
}

// Rows returns the total surviving row count across all collected chunks.
func (c *Collector) Rows() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, b := range c.batches {
		n += b.Len
	}
	return n
}

// ToRows expands each chunk into one row Record per row (Payload built from the
// columns), for handoff to the row path or a JSON/row sink. This re-introduces
// map[string]any allocation, so use it only at egress. Int64/Float64 columns are
// supported; other kinds are skipped.
func ToRows() core.Operator { return rowExpand{} }

type rowExpand struct{}

func (rowExpand) Process(in []core.Record) ([]core.Record, error) {
	var out []core.Record
	for _, r := range in {
		b := r.Chunk
		if b == nil {
			out = append(out, r)
			continue
		}
		for i := 0; i < b.Len; i++ {
			payload := make(map[string]any, len(b.Cols))
			for ci := range b.Cols {
				name := b.Schema.Fields[ci].Name
				if b.Cols[ci].Null != nil && b.Cols[ci].Null[i] {
					payload[name] = nil // NULL cell → absent value on the row path
					continue
				}
				switch b.Cols[ci].Kind {
				case core.KindInt64:
					payload[name] = b.Cols[ci].I64[i]
				case core.KindFloat64:
					payload[name] = b.Cols[ci].F64[i]
				case core.KindString:
					payload[name] = b.Cols[ci].Str[i]
				case core.KindBool:
					payload[name] = b.Cols[ci].B[i]
				}
			}
			out = append(out, core.Record{SchemaID: b.Schema.ID, SchemaVersion: b.Schema.Version, Payload: payload})
		}
	}
	return out, nil
}
func (rowExpand) OnSchemaChange(core.Schema) {}
