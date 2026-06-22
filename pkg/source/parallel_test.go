package source_test

import (
	"context"
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/source"
)

func recsRange(lo, hi int) []core.Record {
	out := make([]core.Record, 0, hi-lo)
	for i := lo; i < hi; i++ {
		out = append(out, core.Record{Payload: map[string]any{"v": i}})
	}
	return out
}

func drain(t *testing.T, src core.Source, ctx context.Context) []core.Record {
	t.Helper()
	ch, err := src.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var got []core.Record
	for r := range ch {
		got = append(got, r)
	}
	return got
}

func TestParallel_FansInAll(t *testing.T) {
	p := source.NewParallel(
		source.NewMemory(recsRange(0, 100)),
		source.NewMemory(recsRange(100, 250)),
		source.NewMemory(recsRange(250, 300)),
	)
	got := drain(t, p, context.Background())
	if len(got) != 300 {
		t.Fatalf("got %d records, want 300", len(got))
	}
	// Every value 0..299 exactly once (order-agnostic).
	seen := make(map[int]int, 300)
	for _, r := range got {
		seen[r.Payload["v"].(int)]++
	}
	for i := range 300 {
		if seen[i] != 1 {
			t.Fatalf("value %d seen %d times, want 1", i, seen[i])
		}
	}
}

func TestParallel_Empty(t *testing.T) {
	got := drain(t, source.NewParallel(), context.Background())
	if len(got) != 0 {
		t.Fatalf("got %d, want 0", len(got))
	}
}

func TestParallel_RespectsCancel(t *testing.T) {
	// A generator never closes; Parallel must stop when ctx is cancelled.
	gen := source.NewGenerator(func(seq int) core.Record {
		return core.Record{Payload: map[string]any{"v": seq}}
	}, time.Millisecond)
	p := source.NewParallel(gen, source.NewMemory(recsRange(0, 10)))

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := p.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// consume a few, then cancel and ensure the channel closes.
	for range 5 {
		<-ch
	}
	cancel()
	closed := make(chan struct{})
	go func() {
		for range ch { //nolint:revive
		}
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Parallel did not close output channel after cancel")
	}
}
