package source

import (
	"context"
	"fmt"
	"sync"

	"github.com/gribovan2005/drift/pkg/core"
)

// Parallel reads from several sub-sources concurrently and fans their records
// into a single stream. A single core.Source reads on one goroutine, so one Kafka
// topic / generator is a serial ingestion ceiling; Parallel consumes N sources
// (e.g. N Kafka partitions) at once. Cross-source ordering is NOT preserved —
// records interleave by arrival. See drift/Specs/Parallel Source.md.
type Parallel struct {
	subs   []core.Source
	bufSize int
}

// NewParallel creates a Parallel source over subs. With no subs, Read returns an
// immediately-closed channel.
func NewParallel(subs ...core.Source) *Parallel {
	return &Parallel{subs: subs, bufSize: 256}
}

// Read starts every sub-source and fans their channels into one. The output
// closes when all subs drain or ctx is cancelled.
func (p *Parallel) Read(ctx context.Context) (<-chan core.Record, error) {
	chans := make([]<-chan core.Record, 0, len(p.subs))
	for i, s := range p.subs {
		ch, err := s.Read(ctx)
		if err != nil {
			return nil, fmt.Errorf("parallel source: sub %d: %w", i, err)
		}
		chans = append(chans, ch)
	}

	out := make(chan core.Record, p.bufSize)
	var wg sync.WaitGroup
	for _, ch := range chans {
		wg.Add(1)
		go func(c <-chan core.Record) {
			defer wg.Done()
			for {
				select {
				case r, ok := <-c:
					if !ok {
						return
					}
					select {
					case out <- r:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(ch)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out, nil
}
