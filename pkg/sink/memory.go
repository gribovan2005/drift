package sink

import (
	"context"
	"sync"

	"github.com/gribovan2005/drift/pkg/core"
)

// Memory collects all records written to it. Safe for concurrent reads
// after Write returns.
type Memory struct {
	mu      sync.Mutex
	records []core.Record
}

// NewMemory creates an empty Memory sink.
func NewMemory() *Memory { return &Memory{} }

func (m *Memory) Write(ctx context.Context, ch <-chan core.Record) error {
	for {
		select {
		case r, ok := <-ch:
			if !ok {
				return nil
			}
			m.mu.Lock()
			m.records = append(m.records, r)
			m.mu.Unlock()
		case <-ctx.Done():
			return nil
		}
	}
}

// Records returns a snapshot of all collected records.
func (m *Memory) Records() []core.Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]core.Record, len(m.records))
	copy(out, m.records)
	return out
}
