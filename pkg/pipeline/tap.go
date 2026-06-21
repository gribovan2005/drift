package pipeline

import (
	"sync"

	"github.com/andrejgribov/drift/pkg/core"
)

// Tap captures the most recent records emitted by each stage so the UI can show
// actual data flowing through the pipeline (not just metrics). It keeps a bounded
// ring of the last N records per stage label. Safe for concurrent use: each stage
// goroutine records its own outputs while the web server reads.
type Tap struct {
	n   int
	mu  sync.RWMutex
	buf map[string][]core.Record
}

// NewTap creates a Tap retaining the last n records per stage.
func NewTap(n int) *Tap {
	if n < 1 {
		n = 1
	}
	return &Tap{n: n, buf: make(map[string][]core.Record)}
}

// record appends a stage's emitted records, trimming to the last n.
func (t *Tap) record(stage string, rs []core.Record) {
	if t == nil || len(rs) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	b := append(t.buf[stage], rs...)
	if len(b) > t.n {
		b = append(b[:0:0], b[len(b)-t.n:]...) // keep last n, drop backing array
	}
	t.buf[stage] = b
}

// Sample returns up to n recent records emitted by stage (newest last).
func (t *Tap) Sample(stage string) []core.Record {
	t.mu.RLock()
	defer t.mu.RUnlock()
	b := t.buf[stage]
	out := make([]core.Record, len(b))
	copy(out, b)
	return out
}

// WithTap captures recent per-stage output records into t for inspection.
func WithTap(t *Tap) Option {
	return func(p *Pipeline) { p.tap = t }
}
