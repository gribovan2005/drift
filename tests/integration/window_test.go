package integration

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/operator"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/sink"
	"github.com/gribovan2005/drift/pkg/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sumWindow(window []core.Record) (core.Record, error) {
	sum := 0
	for _, r := range window {
		sum += r.Payload["v"].(int)
	}
	return core.Record{Payload: map[string]any{
		"sum":   sum,
		"count": len(window),
	}}, nil
}

// TestPipeline_TumblingWindow verifies that full windows are emitted and
// the partial tail is flushed when the source closes.
func TestPipeline_TumblingWindow(t *testing.T) {
	// 7 records with window size 3 → 2 full windows + 1 partial flush.
	records := makeRecords(1, 2, 3, 4, 5, 6, 7)
	src := source.NewMemory(records)
	snk := sink.NewMemory()

	w, err := operator.NewTumblingWindow(3, sumWindow)
	require.NoError(t, err)

	p := pipeline.New(src, []pipeline.Stage{{Label: "tumble", Op: w}}, snk)
	require.NoError(t, p.Run(context.Background()))

	got := snk.Records()
	require.Len(t, got, 3) // 2 full + 1 partial

	// Full window 1: 1+2+3=6
	assert.Equal(t, 6, got[0].Payload["sum"])
	assert.Equal(t, 3, got[0].Payload["count"])

	// Full window 2: 4+5+6=15
	assert.Equal(t, 15, got[1].Payload["sum"])
	assert.Equal(t, 3, got[1].Payload["count"])

	// Partial flush: 7
	assert.Equal(t, 7, got[2].Payload["sum"])
	assert.Equal(t, 1, got[2].Payload["count"])
}

// TestPipeline_SlidingWindow verifies overlapping windows E2E.
func TestPipeline_SlidingWindow(t *testing.T) {
	// size=4, step=2, 6 records → 3 emissions.
	records := makeRecords(1, 2, 3, 4, 5, 6)
	src := source.NewMemory(records)
	snk := sink.NewMemory()

	w, err := operator.NewSlidingWindow(4, 2, sumWindow)
	require.NoError(t, err)

	p := pipeline.New(src, []pipeline.Stage{{Label: "slide", Op: w}}, snk)
	require.NoError(t, p.Run(context.Background()))

	got := snk.Records()
	require.Len(t, got, 3)

	// After record 2: window=[1,2], sum=3
	assert.Equal(t, 3, got[0].Payload["sum"])
	// After record 4: window=[1,2,3,4], sum=10
	assert.Equal(t, 10, got[1].Payload["sum"])
	// After record 6: window=[3,4,5,6], sum=18
	assert.Equal(t, 18, got[2].Payload["sum"])
}

// TestPipeline_WindowChained: filter → tumbling window.
func TestPipeline_WindowChained(t *testing.T) {
	// Keep only even numbers, then aggregate in windows of 2.
	records := makeRecords(1, 2, 3, 4, 5, 6, 7, 8)
	src := source.NewMemory(records)
	snk := sink.NewMemory()

	w, _ := operator.NewTumblingWindow(2, sumWindow)
	stages := []pipeline.Stage{
		{
			Label: "evens",
			Op: operator.NewFilter(func(r core.Record) bool {
				return r.Payload["v"].(int)%2 == 0
			}),
		},
		{Label: "window", Op: w},
	}

	p := pipeline.New(src, stages, snk)
	require.NoError(t, p.Run(context.Background()))

	// Even records: 2,4,6,8 → windows: [2,4]=6, [6,8]=14
	got := snk.Records()
	require.Len(t, got, 2)
	assert.Equal(t, 6, got[0].Payload["sum"])
	assert.Equal(t, 14, got[1].Payload["sum"])
}
