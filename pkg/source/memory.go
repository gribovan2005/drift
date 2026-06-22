package source

import (
	"context"

	"github.com/gribovan2005/drift/pkg/core"
)

// Memory is an in-memory Source that emits a fixed slice of records.
// Useful for tests and demos.
type Memory struct {
	records []core.Record
}

// NewMemory creates a Memory source from the given records.
func NewMemory(records []core.Record) *Memory {
	return &Memory{records: records}
}

func (m *Memory) Read(ctx context.Context) (<-chan core.Record, error) {
	ch := make(chan core.Record, len(m.records))
	go func() {
		defer close(ch)
		for _, r := range m.records {
			select {
			case ch <- r:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}
