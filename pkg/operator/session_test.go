package operator

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/andrejgribov/drift/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// swBase is an arbitrary instant used as t0 for session-window tests.
var swBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// swCountAgg aggregates a session into one record carrying its size, key, and
// the first record's event time (for ordering assertions).
func swCountAgg(window []core.Record) (core.Record, error) {
	return core.Record{
		Payload: map[string]any{
			"count": len(window),
			"key":   window[0].Payload["k"],
		},
		EventTime: window[0].EventTime,
	}, nil
}

func swKey(r core.Record) string { return r.Payload["k"].(string) }

// swRec builds a record with key k and an event time of t0+secs.
func swRec(k string, secs int) core.Record {
	return core.Record{
		Payload:   map[string]any{"k": k},
		EventTime: swBase.Add(time.Duration(secs) * time.Second),
	}
}

func TestSessionWindow_InvalidGap(t *testing.T) {
	_, err := NewSessionWindow(0, swKey, swCountAgg)
	assert.Error(t, err)
}

func TestSessionWindow_NilKeyFn(t *testing.T) {
	_, err := NewSessionWindow(time.Second, nil, swCountAgg)
	assert.Error(t, err)
}

func TestSessionWindow_FiresAfterGap(t *testing.T) {
	// gap=5s. Events at 0,2,4 form one session; a later event at 20 advances the
	// watermark past 4+5=9, closing the session.
	w, err := NewSessionWindow(5*time.Second, swKey, swCountAgg)
	require.NoError(t, err)

	out, err := w.Process([]core.Record{
		swRec("a", 0), swRec("a", 2), swRec("a", 4),
	})
	require.NoError(t, err)
	assert.Empty(t, out, "session still open — watermark at 4")

	out, err = w.Process([]core.Record{swRec("a", 20)})
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, 3, out[0].Payload["count"])
	assert.Equal(t, swBase, out[0].EventTime)
}

func TestSessionWindow_SeparateSessionsByGap(t *testing.T) {
	// gap=5s. Events 0,2 (session 1) then 100,102 (session 2). A trailing event
	// closes session 2 too.
	w, err := NewSessionWindow(5*time.Second, swKey, swCountAgg)
	require.NoError(t, err)

	out, _ := w.Process([]core.Record{
		swRec("a", 0), swRec("a", 2), // session 1
		swRec("a", 100), swRec("a", 102), // session 2 (gap 98 > 5 closes session 1)
	})
	// Session 1 closed (watermark 102 > 2+5); session 2 still open.
	require.Len(t, out, 1)
	assert.Equal(t, 2, out[0].Payload["count"])
	assert.Equal(t, swBase, out[0].EventTime)

	// Flush closes session 2.
	out, _ = w.Flush()
	require.Len(t, out, 1)
	assert.Equal(t, 2, out[0].Payload["count"])
	assert.Equal(t, swBase.Add(100*time.Second), out[0].EventTime)
}

func TestSessionWindow_KeyedIndependence(t *testing.T) {
	// Two keys, interleaved; each maintains its own session.
	w, err := NewSessionWindow(5*time.Second, swKey, swCountAgg)
	require.NoError(t, err)

	w.Process([]core.Record{
		swRec("a", 0), swRec("b", 1), swRec("a", 2), swRec("b", 3),
	})
	// Close both by jumping far ahead on a third key-agnostic advance.
	out, err := w.Process([]core.Record{swRec("a", 50)})
	require.NoError(t, err)
	// "a" session [0,2] closes (50 > 2+5); the new "a" event at 50 opens a new
	// session. "b" session [1,3] closes too (50 > 3+5).
	require.Len(t, out, 2)
	// Ascending start order: a@0 then b@1.
	assert.Equal(t, "a", out[0].Payload["key"])
	assert.Equal(t, 2, out[0].Payload["count"])
	assert.Equal(t, "b", out[1].Payload["key"])
	assert.Equal(t, 2, out[1].Payload["count"])
}

func TestSessionWindow_OutOfOrderExtendsSession(t *testing.T) {
	// gap=5s. Events 0, then 8 (gap 8 > 5 → new session), then 4 bridges them
	// (4 within gap of both) → all merge into one session.
	w, err := NewSessionWindow(5*time.Second, swKey, swCountAgg)
	require.NoError(t, err)

	w.Process([]core.Record{swRec("a", 0), swRec("a", 8), swRec("a", 4)})
	out, err := w.Flush()
	require.NoError(t, err)
	require.Len(t, out, 1, "bridged sessions merge into one")
	assert.Equal(t, 3, out[0].Payload["count"])
}

func TestSessionWindow_DropsLateRecords(t *testing.T) {
	w, err := NewSessionWindow(5*time.Second, swKey, swCountAgg)
	require.NoError(t, err)

	// Close session [0,2] by advancing to 30.
	w.Process([]core.Record{swRec("a", 0), swRec("a", 2), swRec("a", 30)})
	assert.Equal(t, int64(0), w.LateDropped())

	// A record at t=1 can no longer form/extend a fired session → late.
	out, err := w.Process([]core.Record{swRec("a", 1)})
	require.NoError(t, err)
	assert.Empty(t, out)
	assert.Equal(t, int64(1), w.LateDropped())
}

func TestSessionWindow_Flush(t *testing.T) {
	w, err := NewSessionWindow(5*time.Second, swKey, swCountAgg)
	require.NoError(t, err)

	w.Process([]core.Record{swRec("a", 0), swRec("b", 1)})
	out, err := w.Flush()
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, "a", out[0].Payload["key"])
	assert.Equal(t, "b", out[1].Payload["key"])

	out, err = w.Flush()
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestSessionWindow_WatermarkAccessor(t *testing.T) {
	w, err := NewSessionWindow(5*time.Second, swKey, swCountAgg)
	require.NoError(t, err)
	assert.True(t, w.Watermark().IsZero())

	w.Process([]core.Record{swRec("a", 7)})
	assert.Equal(t, swBase.Add(7*time.Second), w.Watermark())
}

func TestSessionWindow_EmptyInput(t *testing.T) {
	w, err := NewSessionWindow(5*time.Second, swKey, swCountAgg)
	require.NoError(t, err)
	out, err := w.Process(nil)
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestSessionWindow_Snapshot_Restore(t *testing.T) {
	w1, err := NewSessionWindow(5*time.Second, swKey, swCountAgg)
	require.NoError(t, err)
	w1.Process([]core.Record{swRec("a", 0), swRec("a", 2)}) // open session

	data, err := w1.Snapshot()
	require.NoError(t, err)

	w2, err := NewSessionWindow(5*time.Second, swKey, swCountAgg)
	require.NoError(t, err)
	require.NoError(t, w2.Restore(data))

	// Restored session [0,2] fires when an event at t=30 (a separate session)
	// advances the watermark past 2+5.
	out, err := w2.Process([]core.Record{swRec("a", 30)})
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, 2, out[0].Payload["count"])
}

func TestSessionWindow_Restore_InvalidData(t *testing.T) {
	w, err := NewSessionWindow(5*time.Second, swKey, swCountAgg)
	require.NoError(t, err)
	assert.Error(t, w.Restore([]byte("not json")))
}

func TestSessionWindow_AggregateError(t *testing.T) {
	boom := func([]core.Record) (core.Record, error) { return core.Record{}, fmt.Errorf("boom") }
	w, err := NewSessionWindow(5*time.Second, swKey, boom)
	require.NoError(t, err)
	_, err = w.Process([]core.Record{swRec("a", 0), swRec("a", 30)})
	assert.ErrorContains(t, err, "boom")
}

// TestSessionWindow_OnSchemaChange_Concurrent ensures OnSchemaChange on another
// goroutine does not race with Process. Run with -race.
func TestSessionWindow_OnSchemaChange_Concurrent(t *testing.T) {
	w, err := NewSessionWindow(5*time.Second, swKey, swCountAgg)
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
			w.Process([]core.Record{swRec("a", i)})
		}
	}()
	wg.Wait()
}
