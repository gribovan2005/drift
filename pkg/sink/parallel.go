package sink

import (
	"context"
	"sync"

	"github.com/gribovan2005/drift/pkg/core"
)

// Parallel fans the incoming records round-robin to n inner sinks (one per mk()),
// each running on its own goroutine. It is the mirror of source.NewParallel and
// removes the single-sink serial point on the data path: with a parallel source +
// vector.Parallel stage + this, a columnar lane scales with cores.
//
// Round-robin distribution suits stateless sinks; a stateful/keyed sink should be
// sharded by key upstream instead. See drift/Specs/Parallel Source.md.
func Parallel(n int, mk func() core.Sink) core.Sink {
	if n < 1 {
		n = 1
	}
	return &parallel{n: n, mk: mk, bufSize: 64}
}

type parallel struct {
	n       int
	mk      func() core.Sink
	bufSize int
}

func (p *parallel) Write(ctx context.Context, ch <-chan core.Record) error {
	// Child context so an inner sink's error (or parent cancel) unblocks the
	// dispatcher even if a sub-channel's reader has exited.
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	subs := make([]chan core.Record, p.n)
	errs := make([]error, p.n)
	var wg sync.WaitGroup
	for i := 0; i < p.n; i++ {
		subs[i] = make(chan core.Record, p.bufSize)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := p.mk().Write(cctx, subs[i]); err != nil {
				errs[i] = err
				cancel()
			}
		}(i)
	}

	i := 0
dispatch:
	for {
		select {
		case <-cctx.Done():
			break dispatch
		case r, ok := <-ch:
			if !ok {
				break dispatch
			}
			select {
			case subs[i%p.n] <- r:
				i++
			case <-cctx.Done():
				break dispatch
			}
		}
	}
	for _, s := range subs {
		close(s)
	}
	wg.Wait()

	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return ctx.Err() // nil on normal completion; set if the parent ctx was cancelled
}
