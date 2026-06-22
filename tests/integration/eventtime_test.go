package integration

import (
	"context"
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/operator"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/sink"
	"github.com/gribovan2005/drift/pkg/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPipeline_EventTime_TumblingByWatermark runs TimestampAssigner →
// EventTimeWindow through the real pipeline. Records carry a "ts" field (seconds
// from t0); the window groups by 10s of event time and counts per window.
// Flush() on stream end fires any windows still open.
func TestPipeline_EventTime_TumblingByWatermark(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Event times (seconds): 1,2,3 → window [0,10); 11,12 → [10,20); 25 → [20,30).
	secs := []int{1, 2, 3, 11, 12, 25}
	recs := make([]core.Record, len(secs))
	for i, s := range secs {
		recs[i] = core.Record{Payload: map[string]any{"ts": s}}
	}

	assigner := operator.NewTimestampAssigner(func(r core.Record) time.Time {
		return t0.Add(time.Duration(r.Payload["ts"].(int)) * time.Second)
	})
	win, err := operator.NewEventTimeWindow(10*time.Second, 0, func(w []core.Record) (core.Record, error) {
		return core.Record{Payload: map[string]any{"count": len(w)}, EventTime: w[0].EventTime}, nil
	})
	require.NoError(t, err)

	src := source.NewMemory(recs)
	snk := sink.NewMemory()
	p := pipeline.New(src, []pipeline.Stage{
		{Label: "assign-ts", Op: assigner, Next: []string{"window"}},
		{Label: "window", Op: win},
	}, snk)
	require.NoError(t, p.Run(context.Background()))

	got := snk.Records()
	// Three windows fire: [0,10)=3, [10,20)=2, [20,30)=1.
	require.Len(t, got, 3)

	counts := make([]int, len(got))
	for i, r := range got {
		counts[i] = r.Payload["count"].(int)
	}
	assert.Equal(t, []int{3, 2, 1}, counts)
}

// TestPipeline_SessionWindow_GapBased runs TimestampAssigner → SessionWindow.
// One key "u", events cluster into two sessions separated by a >gap idle period.
func TestPipeline_SessionWindow_GapBased(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Session 1: 0,2,4. Idle. Session 2: 60,62. (gap=10s)
	secs := []int{0, 2, 4, 60, 62}
	recs := make([]core.Record, len(secs))
	for i, s := range secs {
		recs[i] = core.Record{Payload: map[string]any{"ts": s, "u": "user1"}}
	}

	assigner := operator.NewTimestampAssigner(func(r core.Record) time.Time {
		return t0.Add(time.Duration(r.Payload["ts"].(int)) * time.Second)
	})
	win, err := operator.NewSessionWindow(10*time.Second,
		func(r core.Record) string { return r.Payload["u"].(string) },
		func(w []core.Record) (core.Record, error) {
			return core.Record{Payload: map[string]any{"count": len(w)}, EventTime: w[0].EventTime}, nil
		})
	require.NoError(t, err)

	src := source.NewMemory(recs)
	snk := sink.NewMemory()
	p := pipeline.New(src, []pipeline.Stage{
		{Label: "assign-ts", Op: assigner, Next: []string{"session"}},
		{Label: "session", Op: win},
	}, snk)
	require.NoError(t, p.Run(context.Background()))

	got := snk.Records()
	require.Len(t, got, 2, "two sessions: [0,4] and [60,62]")
	assert.Equal(t, 3, got[0].Payload["count"])
	assert.Equal(t, 2, got[1].Payload["count"])
}
