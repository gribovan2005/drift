package source

import (
	"context"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
)

// GeneratorFunc produces the next Record given a monotonically increasing
// sequence number.
type GeneratorFunc func(seq int) core.Record

// Generator emits records produced by fn at the given rate. It runs until
// ctx is cancelled.
type Generator struct {
	fn       GeneratorFunc
	interval time.Duration // time between records; 0 = as fast as possible
}

// NewGenerator creates a Generator source.
// interval controls emission rate; pass 0 for maximum throughput.
func NewGenerator(fn GeneratorFunc, interval time.Duration) *Generator {
	return &Generator{fn: fn, interval: interval}
}

func (g *Generator) Read(ctx context.Context) (<-chan core.Record, error) {
	ch := make(chan core.Record, 256)
	go func() {
		defer close(ch)
		var ticker *time.Ticker
		var tickC <-chan time.Time
		if g.interval > 0 {
			ticker = time.NewTicker(g.interval)
			tickC = ticker.C
			defer ticker.Stop()
		}

		for seq := 0; ; seq++ {
			if g.interval > 0 {
				select {
				case <-tickC:
				case <-ctx.Done():
					return
				}
			}
			select {
			case ch <- g.fn(seq):
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}
