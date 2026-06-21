package bench

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/andrejgribov/drift/pkg/core"
	"github.com/andrejgribov/drift/pkg/operator"
	"github.com/andrejgribov/drift/pkg/pipeline"
	"github.com/andrejgribov/drift/pkg/sink"
	"github.com/andrejgribov/drift/pkg/source"
	"github.com/stretchr/testify/assert"
)

// minThroughputRecPerSec is the floor for the map+filter pipeline.
// Set conservatively (50k rec/sec) — the M3 baseline is ~9M rec/sec.
// This catches O(n²) regressions or channel/goroutine deadlocks, not minor slowdowns.
// Update with `make bench-baseline` if the architecture changes significantly.
const minThroughputRecPerSec = 50_000

// TestPipelineThroughputFloor runs a fixed workload and asserts a minimum
// throughput. It is the CI regression gate — failure means a severe slowdown.
func TestPipelineThroughputFloor(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput floor in -short mode")
	}

	const n = 50_000
	records := makeBenchRecords(n)

	stages := []pipeline.Stage{
		{
			Label: "filter",
			Op: operator.NewFilter(func(r core.Record) bool {
				return r.Payload["v"].(int)%2 == 0
			}),
		},
		{
			Label: "map",
			Op: operator.NewMap(func(r core.Record) (core.Record, error) {
				r.Payload["v"] = r.Payload["v"].(int) + 1
				return r, nil
			}),
		},
	}

	start := time.Now()
	src := source.NewMemory(records)
	snk := sink.NewMemory()
	p := pipeline.New(src, stages, snk)
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("pipeline error: %v", err)
	}
	elapsed := time.Since(start)

	recPerSec := float64(n) / elapsed.Seconds()
	t.Logf("throughput: %.0f records/sec (%.2f ms for %d records)", recPerSec, elapsed.Seconds()*1000, n)
	assert.GreaterOrEqualf(t, recPerSec, float64(minThroughputRecPerSec),
		"throughput %.0f rec/sec fell below regression floor %d rec/sec", recPerSec, minThroughputRecPerSec)
}

// TestDedupThroughputFloor asserts minimum throughput for a Deduplicate pipeline.
const minDedupThroughputRecPerSec = 30_000

func TestDedupThroughputFloor(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput floor in -short mode")
	}

	const n = 50_000
	records := makeBenchRecords(n)
	op := operator.NewDeduplicate(func(r core.Record) string {
		return fmt.Sprintf("%d", r.Payload["v"].(int))
	}, time.Hour)

	start := time.Now()
	src := source.NewMemory(records)
	snk := sink.NewMemory()
	p := pipeline.New(src, []pipeline.Stage{{Label: "dedup", Op: op}}, snk)
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("pipeline error: %v", err)
	}
	elapsed := time.Since(start)

	recPerSec := float64(n) / elapsed.Seconds()
	t.Logf("dedup throughput: %.0f records/sec", recPerSec)
	assert.GreaterOrEqualf(t, recPerSec, float64(minDedupThroughputRecPerSec),
		"dedup throughput %.0f rec/sec fell below floor %d rec/sec", recPerSec, minDedupThroughputRecPerSec)
}

// TestWindowThroughputFloor asserts minimum throughput for a tumbling window pipeline.
const minWindowThroughputRecPerSec = 30_000

func TestWindowThroughputFloor(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput floor in -short mode")
	}

	const n = 50_000
	records := makeBenchRecords(n)
	w, _ := operator.NewTumblingWindow(64, func(win []core.Record) (core.Record, error) {
		return core.Record{Payload: map[string]any{"count": len(win)}}, nil
	})

	start := time.Now()
	src := source.NewMemory(records)
	snk := sink.NewMemory()
	p := pipeline.New(src, []pipeline.Stage{{Label: "window", Op: w}}, snk)
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("pipeline error: %v", err)
	}
	elapsed := time.Since(start)

	recPerSec := float64(n) / elapsed.Seconds()
	t.Logf("window throughput: %.0f records/sec", recPerSec)
	assert.GreaterOrEqualf(t, recPerSec, float64(minWindowThroughputRecPerSec),
		"window throughput %.0f rec/sec fell below floor %d rec/sec", recPerSec, minWindowThroughputRecPerSec)
}
