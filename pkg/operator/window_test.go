package operator

import (
	"errors"
	"testing"

	"github.com/andrejgribov/drift/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sumAgg sums the "v" field across a window.
func sumAgg(window []core.Record) (core.Record, error) {
	sum := 0
	for _, r := range window {
		sum += r.Payload["v"].(int)
	}
	return core.Record{Payload: map[string]any{"sum": sum, "count": len(window)}}, nil
}

func intRecords(vals ...int) []core.Record {
	recs := make([]core.Record, len(vals))
	for i, v := range vals {
		recs[i] = core.Record{Payload: map[string]any{"v": v}}
	}
	return recs
}

// ── TumblingWindow ────────────────────────────────────────────────────────

func TestTumbling_EmitsOnFullWindow(t *testing.T) {
	w, err := NewTumblingWindow(3, sumAgg)
	require.NoError(t, err)

	out, err := w.Process(intRecords(1, 2, 3, 4, 5, 6))
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, 6, out[0].Payload["sum"])  // 1+2+3
	assert.Equal(t, 15, out[1].Payload["sum"]) // 4+5+6
}

func TestTumbling_FlushesPartialWindow(t *testing.T) {
	w, _ := NewTumblingWindow(3, sumAgg)

	// 5 records → one full window (1,2,3) + partial (4,5)
	out, err := w.Process(intRecords(1, 2, 3, 4, 5))
	require.NoError(t, err)
	require.Len(t, out, 1)

	flushed, err := w.Flush()
	require.NoError(t, err)
	require.Len(t, flushed, 1)
	assert.Equal(t, 9, flushed[0].Payload["sum"]) // 4+5
}

func TestTumbling_FlushEmpty(t *testing.T) {
	w, _ := NewTumblingWindow(3, sumAgg)
	flushed, err := w.Flush()
	require.NoError(t, err)
	assert.Empty(t, flushed)
}

func TestTumbling_MultiBatchAccumulation(t *testing.T) {
	// Records arrive across multiple Process calls — window should still close.
	w, _ := NewTumblingWindow(3, sumAgg)

	out1, _ := w.Process(intRecords(1, 2))
	assert.Empty(t, out1) // window not yet full

	out2, _ := w.Process(intRecords(3, 4))
	require.Len(t, out2, 1)
	assert.Equal(t, 6, out2[0].Payload["sum"]) // 1+2+3

	flushed, _ := w.Flush()
	require.Len(t, flushed, 1)
	assert.Equal(t, 4, flushed[0].Payload["sum"]) // partial: 4
}

func TestTumbling_InvalidSize(t *testing.T) {
	_, err := NewTumblingWindow(0, sumAgg)
	assert.Error(t, err)
}

func TestTumbling_AggregatorError(t *testing.T) {
	boom := errors.New("agg failed")
	w, _ := NewTumblingWindow(2, func(_ []core.Record) (core.Record, error) {
		return core.Record{}, boom
	})
	_, err := w.Process(intRecords(1, 2))
	assert.ErrorIs(t, err, boom)
}

// ── SlidingWindow ─────────────────────────────────────────────────────────

func TestSliding_OverlappingWindows(t *testing.T) {
	// size=4, step=2: emit after every 2 records using last 4 as window.
	w, err := NewSlidingWindow(4, 2, sumAgg)
	require.NoError(t, err)

	out, err := w.Process(intRecords(1, 2, 3, 4, 5, 6))
	require.NoError(t, err)
	// After record 2: window=[1,2], sum=3
	// After record 4: window=[1,2,3,4], sum=10
	// After record 6: window=[3,4,5,6], sum=18
	require.Len(t, out, 3)
	assert.Equal(t, 3, out[0].Payload["sum"])
	assert.Equal(t, 10, out[1].Payload["sum"])
	assert.Equal(t, 18, out[2].Payload["sum"])
}

func TestSliding_TumblingEquivalent(t *testing.T) {
	// size == step → behaves like a tumbling window.
	w, _ := NewSlidingWindow(3, 3, sumAgg)
	out, err := w.Process(intRecords(1, 2, 3, 4, 5, 6))
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, 6, out[0].Payload["sum"])
	assert.Equal(t, 15, out[1].Payload["sum"])
}

func TestSliding_FlushesPartialStep(t *testing.T) {
	// size=4, step=3: records 1,2,3 trigger first emission; record 4 starts
	// a new step. Flush emits the partial step with buf=[1,2,3,4] (4 records).
	w, _ := NewSlidingWindow(4, 3, sumAgg)
	out, _ := w.Process(intRecords(1, 2, 3, 4))
	require.Len(t, out, 1) // emitted at count==step (record 3)

	flushed, err := w.Flush()
	require.NoError(t, err)
	require.Len(t, flushed, 1)
	assert.Equal(t, 4, flushed[0].Payload["count"]) // buf holds [1,2,3,4]
	assert.Equal(t, 10, flushed[0].Payload["sum"])  // 1+2+3+4
}

func TestSliding_FlushNoPartial(t *testing.T) {
	// Exactly step records — Flush should return nothing.
	w, _ := NewSlidingWindow(3, 3, sumAgg)
	_, _ = w.Process(intRecords(1, 2, 3))
	flushed, err := w.Flush()
	require.NoError(t, err)
	assert.Empty(t, flushed)
}

func TestSliding_InvalidParams(t *testing.T) {
	_, err := NewSlidingWindow(2, 3, sumAgg) // step > size
	assert.Error(t, err)

	_, err = NewSlidingWindow(3, 0, sumAgg) // step < 1
	assert.Error(t, err)
}

func TestSliding_WindowContentCorrect(t *testing.T) {
	// Verify that after trimming, the window contains the most recent records.
	var captured []core.Record
	w, _ := NewSlidingWindow(3, 3, func(win []core.Record) (core.Record, error) {
		captured = append(captured, win...)
		return core.Record{Payload: map[string]any{"n": len(win)}}, nil
	})

	w.Process(intRecords(10, 20, 30, 40, 50, 60))
	// First window: [10,20,30], second window: [40,50,60]
	assert.Equal(t, 10, captured[0].Payload["v"])
	assert.Equal(t, 60, captured[5].Payload["v"])
}
