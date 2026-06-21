package nexmark

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/andrejgribov/drift/pkg/core"
	"github.com/andrejgribov/drift/pkg/pipeline"
	"github.com/andrejgribov/drift/pkg/sink"
	"github.com/andrejgribov/drift/pkg/source"
)

// runQuery runs a query's stages over the given events and returns wall-clock
// elapsed plus the pipeline metrics snapshot (for latency percentiles).
func runQuery(tb testing.TB, q Query, events int) (time.Duration, pipeline.GraphNode, time.Duration, time.Duration) {
	var recs []core.Record
	if q.Mixed {
		recs = GenerateEvents(events)
	} else {
		recs = GenerateBids(events)
	}
	src := source.NewMemory(recs)
	var snk core.Sink = sink.NewMemory()
	if q.FileSink {
		snk = sink.NewFile(filepath.Join(tb.TempDir(), "q10.ndjson"))
	}
	p := pipeline.New(src, q.Stages(), snk)

	start := time.Now()
	if err := p.Run(context.Background()); err != nil {
		tb.Fatal(err)
	}
	elapsed := time.Since(start)
	snap := p.Snapshot()
	var p50, p99 time.Duration
	if len(snap.Stages) > 0 {
		p50 = snap.Stages[0].LatencyP50
		p99 = snap.Stages[0].LatencyP99
	}
	return elapsed, pipeline.GraphNode{}, p50, p99
}

// TestNexmarkThroughput runs each implemented query and reports events/sec and
// per-batch latency. Run with:  go test ./tests/nexmark/ -run Throughput -v
func TestNexmarkThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("throughput run skipped in -short")
	}
	const events = 2_000_000

	t.Logf("Nexmark on Drift — %d bid events, single process\n", events)
	t.Logf("%-5s  %-38s  %12s  %10s  %10s", "query", "description", "events/sec", "p50", "p99")
	for _, q := range Implemented() {
		elapsed, _, p50, p99 := runQuery(t, q, events)
		eps := float64(events) / elapsed.Seconds()
		t.Logf("%-5s  %-38s  %12s  %10s  %10s", q.ID, q.Desc, humanRate(eps), p50.Round(time.Microsecond), p99.Round(time.Microsecond))
	}
}

func humanRate(eps float64) string {
	switch {
	case eps >= 1e6:
		return fmt.Sprintf("%.1fM/s", eps/1e6)
	case eps >= 1e3:
		return fmt.Sprintf("%.1fk/s", eps/1e3)
	default:
		return fmt.Sprintf("%.0f/s", eps)
	}
}

// ── Go benchmarks (go test -bench Nexmark -benchmem) ────────────────────────

func benchQuery(b *testing.B, stages []pipeline.Stage) {
	const events = 200_000
	recs := GenerateBids(events)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		src := source.NewMemory(recs)
		snk := sink.NewMemory()
		p := pipeline.New(src, stages, snk)
		if err := p.Run(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(events)) // "MB/s" column reads as events/sec
	}
}

func BenchmarkNexmark_Q0(b *testing.B) { benchQuery(b, Q0()) }
func BenchmarkNexmark_Q1(b *testing.B) { benchQuery(b, Q1()) }
func BenchmarkNexmark_Q2(b *testing.B) { benchQuery(b, Q2()) }
