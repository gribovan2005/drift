package sink_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/sink"
)

func feed(records []core.Record) <-chan core.Record {
	ch := make(chan core.Record, len(records))
	for _, r := range records {
		ch <- r
	}
	close(ch)
	return ch
}

// countSink records how many records it received and the set of values seen.
type countSink struct {
	mu   sync.Mutex
	seen map[int]int
	n    int64
}

func (s *countSink) Write(ctx context.Context, ch <-chan core.Record) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r, ok := <-ch:
			if !ok {
				return nil
			}
			atomic.AddInt64(&s.n, 1)
			s.mu.Lock()
			s.seen[r.Payload["v"].(int)] = s.seen[r.Payload["v"].(int)] + 1
			s.mu.Unlock()
		}
	}
}

func TestParallelSink_FanOutAll(t *testing.T) {
	shared := &countSink{seen: make(map[int]int)}
	// 4 workers all feeding one shared counter (so we can assert totals).
	ps := sink.Parallel(4, func() core.Sink { return shared })

	recs := make([]core.Record, 1000)
	for i := range recs {
		recs[i] = core.Record{Payload: map[string]any{"v": i}}
	}
	if err := ps.Write(context.Background(), feed(recs)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if shared.n != 1000 {
		t.Fatalf("received %d, want 1000", shared.n)
	}
	for i := 0; i < 1000; i++ {
		if shared.seen[i] != 1 {
			t.Fatalf("value %d seen %d times, want 1", i, shared.seen[i])
		}
	}
}

func TestParallelSink_N1(t *testing.T) {
	s := &countSink{seen: make(map[int]int)}
	ps := sink.Parallel(1, func() core.Sink { return s })
	recs := []core.Record{{Payload: map[string]any{"v": 1}}, {Payload: map[string]any{"v": 2}}}
	if err := ps.Write(context.Background(), feed(recs)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if s.n != 2 {
		t.Fatalf("n=%d, want 2", s.n)
	}
}

func TestParallelSink_RespectsCancel(t *testing.T) {
	// inner sinks block (never-closing input handled by ctx); cancel must end Write.
	ps := sink.Parallel(2, func() core.Sink { return &countSink{seen: make(map[int]int)} })
	ch := make(chan core.Record) // never closed
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ps.Write(ctx, ch) }()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ParallelSink did not return after cancel")
	}
}

type errSink struct{}

func (errSink) Write(ctx context.Context, ch <-chan core.Record) error {
	return errors.New("boom")
}

func TestParallelSink_ErrorPropagates(t *testing.T) {
	ps := sink.Parallel(3, func() core.Sink { return errSink{} })
	recs := make([]core.Record, 100)
	for i := range recs {
		recs[i] = core.Record{Payload: map[string]any{"v": i}}
	}
	if err := ps.Write(context.Background(), feed(recs)); err == nil {
		t.Fatal("expected inner-sink error to propagate")
	}
}
