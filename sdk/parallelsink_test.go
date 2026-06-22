package sdk_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/gribovan2005/drift/sdk"
)

type counterSink struct{ n *int64 }

func (c counterSink) Write(ctx context.Context, ch <-chan sdk.Record) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-ch:
			if !ok {
				return nil
			}
			atomic.AddInt64(c.n, 1)
		}
	}
}

func TestParallelSink_EndToEnd(t *testing.T) {
	var n int64
	if err := sdk.New().
		From(sdk.Slice(recs(500))).
		To(sdk.ParallelSink(4, func() sdk.Sink { return counterSink{n: &n} })).
		Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if atomic.LoadInt64(&n) != 500 {
		t.Fatalf("received %d, want 500 across 4 sink workers", n)
	}
}
