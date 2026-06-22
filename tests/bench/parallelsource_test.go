package bench

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/source"
)

// slowSource models a real reader whose per-record cost is CPU-bound (network
// frame decode + JSON unmarshal), unlike the in-memory source. Each record costs
// ~a fixed amount of CPU work, so a single reader is decode-bound and N parallel
// readers overlap that work across cores — exactly the Kafka-partitions case.
type slowSource struct {
	n    int
	work int // iterations of busy-work per record (simulated decode cost)
}

func (s *slowSource) Read(ctx context.Context) (<-chan core.Record, error) {
	ch := make(chan core.Record, 256)
	go func() {
		defer close(ch)
		acc := 0
		for i := 0; i < s.n; i++ {
			for j := 0; j < s.work; j++ { // simulated per-record decode CPU
				acc = (acc*1664525 + 1013904223) & 0x7fffffff
			}
			select {
			case ch <- core.Record{Payload: map[string]any{"v": acc}}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

// TestParallelSourceIngest shows that when ingestion is decode-bound (the real
// Kafka case), reading N partitions in parallel scales throughput across cores —
// the single-reader ceiling is lifted. (An in-memory source has no per-record
// cost to overlap, so Parallel only helps when each reader is slow.)
//
// Run:  go test ./tests/bench/ -run ParallelSourceIngest -v -count=1
func TestParallelSourceIngest(t *testing.T) {
	if testing.Short() {
		t.Skip("ingestion bench skipped in -short")
	}
	if raceEnabled {
		t.Skip("ingestion bench skipped under -race (instrumentation distorts timing)")
	}

	prev := runtime.GOMAXPROCS(runtime.NumCPU())
	defer runtime.GOMAXPROCS(prev)

	const total = 2_000_000
	const decodeWork = 400 // ~decode cost per record

	drainRate := func(src core.Source) float64 {
		ch, err := src.Read(context.Background())
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		start := time.Now()
		n := 0
		for range ch {
			n++
		}
		return float64(n) / time.Since(start).Seconds()
	}
	rate := func(eps float64) string { return fmt.Sprintf("%6.2f M/s", eps/1e6) }

	t.Logf("── Decode-bound ingestion (GOMAXPROCS=%d, %d records) ──", runtime.NumCPU(), total)
	single := drainRate(&slowSource{n: total, work: decodeWork})
	t.Logf("  1 reader:               %s  (1.00x)", rate(single))
	for _, k := range []int{2, 4, 8} {
		per := total / k
		subs := make([]core.Source, k)
		for i := range subs {
			subs[i] = &slowSource{n: per, work: decodeWork}
		}
		r := drainRate(source.NewParallel(subs...))
		t.Logf("  %d partitions (Parallel): %s  (%.2fx)", k, rate(r), r/single)
	}
}
