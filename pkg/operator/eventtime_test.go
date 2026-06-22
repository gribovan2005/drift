package operator

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// etBase is an arbitrary aligned instant used as t0 for tests.
var etBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// etCountAgg aggregates a window into one record carrying the window size and the
// first record's event time (for assertions).
func etCountAgg(window []core.Record) (core.Record, error) {
	return core.Record{
		Payload:   map[string]any{"count": len(window)},
		EventTime: window[0].EventTime,
	}, nil
}

func etRec(et time.Time, v int) core.Record {
	return core.Record{Payload: map[string]any{"v": v}, EventTime: et}
}

// ── TimestampAssigner ──────────────────────────────────────────────────────

func TestTimestampAssigner_SetsEventTime(t *testing.T) {
	ta := NewTimestampAssigner(func(r core.Record) time.Time {
		return etBase.Add(time.Duration(r.Payload["v"].(int)) * time.Second)
	})
	in := []core.Record{
		{Payload: map[string]any{"v": 0}},
		{Payload: map[string]any{"v": 5}},
	}
	out, err := ta.Process(in)
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, etBase, out[0].EventTime)
	assert.Equal(t, etBase.Add(5*time.Second), out[1].EventTime)
}

func TestTimestampAssigner_EmptyInput(t *testing.T) {
	ta := NewTimestampAssigner(func(core.Record) time.Time { return etBase })
	out, err := ta.Process(nil)
	require.NoError(t, err)
	assert.Empty(t, out)
}

// ── EventTimeWindow ────────────────────────────────────────────────────────

func TestEventTimeWindow_InvalidSize(t *testing.T) {
	_, err := NewEventTimeWindow(0, 0, etCountAgg)
	assert.Error(t, err)
}

func TestEventTimeWindow_NegativeLateness(t *testing.T) {
	_, err := NewEventTimeWindow(time.Second, -time.Second, etCountAgg)
	assert.Error(t, err)
}

func TestEventTimeWindow_FiresOnWatermark(t *testing.T) {
	// 10s windows, no lateness. Records in window [0,10): t=0,1,2.
	// A record at t=10 advances the watermark to 10, closing window [0,10).
	w, err := NewEventTimeWindow(10*time.Second, 0, etCountAgg)
	require.NoError(t, err)

	out, err := w.Process([]core.Record{
		etRec(etBase, 1),
		etRec(etBase.Add(1*time.Second), 2),
		etRec(etBase.Add(2*time.Second), 3),
	})
	require.NoError(t, err)
	assert.Empty(t, out, "window [0,10) not yet closed — watermark is at 2s")

	// Push watermark to 10s → window [0,10) fires with 3 records.
	out, err = w.Process([]core.Record{etRec(etBase.Add(10*time.Second), 4)})
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, 3, out[0].Payload["count"])
	assert.Equal(t, etBase, out[0].EventTime)
}

func TestEventTimeWindow_AllowedLatenessHoldsWindowOpen(t *testing.T) {
	// 10s windows, 5s lateness. Watermark = maxSeen - 5s.
	w, err := NewEventTimeWindow(10*time.Second, 5*time.Second, etCountAgg)
	require.NoError(t, err)

	w.Process([]core.Record{etRec(etBase, 1)})

	// Event at t=12 → watermark = 7 < 10, window [0,10) stays open.
	out, _ := w.Process([]core.Record{etRec(etBase.Add(12*time.Second), 2)})
	assert.Empty(t, out)

	// A late-ish event at t=8 still lands in window [0,10) (not yet fired).
	out, _ = w.Process([]core.Record{etRec(etBase.Add(8*time.Second), 3)})
	assert.Empty(t, out)

	// Event at t=16 → watermark = 11 ≥ 10, window [0,10) fires with 2 records
	// (t=0 and t=8).
	out, _ = w.Process([]core.Record{etRec(etBase.Add(16*time.Second), 4)})
	require.Len(t, out, 1)
	assert.Equal(t, 2, out[0].Payload["count"])
}

func TestEventTimeWindow_DropsLateRecords(t *testing.T) {
	w, err := NewEventTimeWindow(10*time.Second, 0, etCountAgg)
	require.NoError(t, err)

	// Close window [0,10) by jumping to t=10.
	w.Process([]core.Record{etRec(etBase, 1), etRec(etBase.Add(10*time.Second), 2)})
	assert.Equal(t, int64(0), w.LateDropped())

	// A record at t=5 is now late (its window [0,10) already fired).
	out, err := w.Process([]core.Record{etRec(etBase.Add(5*time.Second), 3)})
	require.NoError(t, err)
	assert.Empty(t, out)
	assert.Equal(t, int64(1), w.LateDropped())
}

func TestEventTimeWindow_MultipleWindowsFireInOrder(t *testing.T) {
	w, err := NewEventTimeWindow(10*time.Second, 0, etCountAgg)
	require.NoError(t, err)

	// Fill windows [0,10), [10,20), [20,30); then jump to 30 to close first three.
	out, err := w.Process([]core.Record{
		etRec(etBase.Add(1*time.Second), 1),
		etRec(etBase.Add(11*time.Second), 2),
		etRec(etBase.Add(21*time.Second), 3),
		etRec(etBase.Add(30*time.Second), 4), // advances watermark to 30
	})
	require.NoError(t, err)
	require.Len(t, out, 3)
	// Ascending window-start order. etCountAgg carries each window's first
	// record EventTime (t=1, 11, 21).
	assert.Equal(t, etBase.Add(1*time.Second), out[0].EventTime)
	assert.Equal(t, etBase.Add(11*time.Second), out[1].EventTime)
	assert.Equal(t, etBase.Add(21*time.Second), out[2].EventTime)
}

func TestEventTimeWindow_Flush(t *testing.T) {
	w, err := NewEventTimeWindow(10*time.Second, 0, etCountAgg)
	require.NoError(t, err)

	// Records in windows [0,10) and [10,20). The batch watermark reaches 11,
	// so window [0,10) (end=10 ≤ 11) fires during Process; [10,20) stays open.
	out, err := w.Process([]core.Record{
		etRec(etBase.Add(1*time.Second), 1),
		etRec(etBase.Add(11*time.Second), 2),
	})
	require.NoError(t, err)
	require.Len(t, out, 1, "window [0,10) fires during Process")
	assert.Equal(t, etBase.Add(1*time.Second), out[0].EventTime)

	// Flush advances the watermark to +∞ and emits the still-open [10,20).
	out, err = w.Flush()
	require.NoError(t, err)
	require.Len(t, out, 1, "Flush fires all remaining windows")
	assert.Equal(t, etBase.Add(11*time.Second), out[0].EventTime)

	// Flush again → nothing left.
	out, err = w.Flush()
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestEventTimeWindow_WatermarkAccessor(t *testing.T) {
	w, err := NewEventTimeWindow(10*time.Second, 3*time.Second, etCountAgg)
	require.NoError(t, err)
	assert.True(t, w.Watermark().IsZero(), "no records seen yet")

	w.Process([]core.Record{etRec(etBase.Add(20*time.Second), 1)})
	assert.Equal(t, etBase.Add(17*time.Second), w.Watermark())
}

func TestEventTimeWindow_EmptyInput(t *testing.T) {
	w, err := NewEventTimeWindow(10*time.Second, 0, etCountAgg)
	require.NoError(t, err)
	out, err := w.Process(nil)
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestEventTimeWindow_Snapshot_Restore(t *testing.T) {
	w1, err := NewEventTimeWindow(10*time.Second, 0, etCountAgg)
	require.NoError(t, err)
	// Open window [0,10) with two records, plus a late drop.
	w1.Process([]core.Record{etRec(etBase, 1), etRec(etBase.Add(2*time.Second), 2)})

	data, err := w1.Snapshot()
	require.NoError(t, err)

	w2, err := NewEventTimeWindow(10*time.Second, 0, etCountAgg)
	require.NoError(t, err)
	require.NoError(t, w2.Restore(data))

	// Restored window should still fire with both records when watermark passes.
	out, err := w2.Process([]core.Record{etRec(etBase.Add(10*time.Second), 3)})
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, 2, out[0].Payload["count"])
}

func TestEventTimeWindow_Restore_InvalidData(t *testing.T) {
	w, err := NewEventTimeWindow(10*time.Second, 0, etCountAgg)
	require.NoError(t, err)
	assert.Error(t, w.Restore([]byte("not json")))
}

func TestEventTimeWindow_AggregateError(t *testing.T) {
	boom := func([]core.Record) (core.Record, error) { return core.Record{}, fmt.Errorf("boom") }
	w, err := NewEventTimeWindow(10*time.Second, 0, boom)
	require.NoError(t, err)
	_, err = w.Process([]core.Record{etRec(etBase, 1), etRec(etBase.Add(10*time.Second), 2)})
	assert.ErrorContains(t, err, "boom")
}

// TestEventTimeWindow_OnSchemaChange_Concurrent ensures OnSchemaChange running
// on a separate goroutine does not race with Process. Run with -race.
func TestEventTimeWindow_OnSchemaChange_Concurrent(t *testing.T) {
	w, err := NewEventTimeWindow(10*time.Second, 0, etCountAgg)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 1000 {
			w.OnSchemaChange(core.Schema{ID: "s", Version: i})
		}
	}()
	go func() {
		defer wg.Done()
		for i := range 1000 {
			w.Process([]core.Record{etRec(etBase.Add(time.Duration(i)*time.Second), i)})
		}
	}()
	wg.Wait()
}
