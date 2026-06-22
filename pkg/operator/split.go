package operator

import (
	"fmt"

	"github.com/gribovan2005/drift/pkg/core"
)

// RouterFunc maps a record to an output route index in [0, n).
// Values outside [0, n) are clamped to route 0.
type RouterFunc func(core.Record) int

// Split routes each record to one of N output routes via routerFn.
// Route 0 is returned from Process (pipeline-integrated).
// Routes 1..n-1 are written to buffered side channels from Outputs().
//
// Writes to side channels block when full — natural backpressure.
// Callers must drain all side channels and call Close() after the pipeline stops.
type Split struct {
	n        int
	routerFn RouterFunc
	outputs  []chan core.Record
	schema   core.Schema
}

// NewSplit creates a Split operator. n must be ≥ 2; bufSize is the buffer
// capacity for each side channel (routes 1..n-1).
func NewSplit(n int, routerFn RouterFunc, bufSize int) (*Split, error) {
	if n < 2 {
		return nil, fmt.Errorf("Split: n must be ≥ 2, got %d", n)
	}
	outputs := make([]chan core.Record, n-1)
	for i := range outputs {
		outputs[i] = make(chan core.Record, bufSize)
	}
	return &Split{n: n, routerFn: routerFn, outputs: outputs}, nil
}

// Outputs returns read-only side channels for routes 1..n-1.
// Outputs()[i] corresponds to route i+1.
func (s *Split) Outputs() []<-chan core.Record {
	ro := make([]<-chan core.Record, len(s.outputs))
	for i, ch := range s.outputs {
		ro[i] = ch
	}
	return ro
}

func (s *Split) Process(in []core.Record) ([]core.Record, error) {
	var primary []core.Record
	for _, r := range in {
		route := s.routerFn(r)
		if route <= 0 || route >= s.n {
			primary = append(primary, r)
			continue
		}
		s.outputs[route-1] <- r
	}
	return primary, nil
}

func (s *Split) OnSchemaChange(schema core.Schema) { s.schema = schema }

// Close closes all side channels. Call once after the pipeline has stopped.
func (s *Split) Close() {
	for _, ch := range s.outputs {
		close(ch)
	}
}
