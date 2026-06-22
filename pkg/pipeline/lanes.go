package pipeline

import (
	"context"
	"sync"
)

// RunLanes runs N independent pipelines concurrently — each with its own source and
// sink, sharing no channel. This is how Drift scales a single node toward linear:
// instead of one pipeline funnelling through shared channels (which caps below
// linear), run one lane per input shard/partition (the Flink/Kafka-Streams
// task-per-partition model).
//
// Fail-fast: if any lane returns an error, a shared child context is cancelled so
// the others stop, and the first error is returned (else the parent ctx error, or
// nil). Lanes are independent — for correct keyed aggregation across lanes the input
// must be sharded by key (each key in exactly one lane); otherwise each lane
// produces its own partial result. See drift/Architecture/Overview.md.
func RunLanes(ctx context.Context, lanes ...*Pipeline) error {
	if len(lanes) == 0 {
		return nil
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make([]error, len(lanes))
	var wg sync.WaitGroup
	for i, p := range lanes {
		wg.Add(1)
		go func(i int, p *Pipeline) {
			defer wg.Done()
			if err := p.Run(cctx); err != nil {
				errs[i] = err
				cancel() // stop the other lanes
			}
		}(i, p)
	}
	wg.Wait()

	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return ctx.Err()
}
