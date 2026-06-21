package nexmark

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/andrejgribov/drift/pkg/core"
	"github.com/andrejgribov/drift/pkg/pipeline"
)

// boundedBidSource generates n bid records on the fly (no rate limit) then closes.
// This mirrors Flink's datagen source (generation cost is included in the timed
// region, and nothing is materialised up front) for a fair same-iron comparison.
type boundedBidSource struct{ n int }

func (s boundedBidSource) Read(ctx context.Context) (<-chan core.Record, error) {
	ch := make(chan core.Record, 1024)
	go func() {
		defer close(ch)
		for i := 0; i < s.n; i++ {
			select {
			case ch <- bidRecord(i):
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

// discardSink counts and drops every record (matches Flink's blackhole sink).
type discardSink struct{ n int64 }

func (d *discardSink) Write(ctx context.Context, ch <-chan core.Record) error {
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return nil
			}
			d.n++
		case <-ctx.Done():
			return nil
		}
	}
}

// TestSameIron mirrors the Flink datagen→blackhole experiment: generate N bids on
// the fly, run q0/q1/q2, discard output. Reports events/sec at the current
// GOMAXPROCS so it can be pinned (GOMAXPROCS=1) for a per-core number.
//
//	go test ./tests/nexmark/ -run SameIron -v -count=1 -timeout 20m
func TestSameIron(t *testing.T) {
	if testing.Short() {
		t.Skip("same-iron run skipped in -short")
	}
	const events = 50_000_000

	t.Logf("Drift same-iron — %d bids on the fly, GOMAXPROCS=%d", events, runtime.GOMAXPROCS(0))
	for _, q := range []Query{
		{ID: "q0", Stages: Q0}, {ID: "q1", Stages: Q1}, {ID: "q2", Stages: Q2},
	} {
		snk := &discardSink{}
		p := pipeline.New(boundedBidSource{events}, q.Stages(), snk)
		start := time.Now()
		if err := p.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(start)
		eps := float64(events) / elapsed.Seconds()
		t.Logf("%-4s  %8.1f s  %s", q.ID, elapsed.Seconds(), humanRate(eps))
	}
}
