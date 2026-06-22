package operator

import (
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func countAgg(recs []core.Record) (core.Record, error) {
	return core.Record{Payload: map[string]any{"count": len(recs)}}, nil
}

func TestTumblingWindow_Snapshot_Restore(t *testing.T) {
	w, err := NewTumblingWindow(4, countAgg)
	require.NoError(t, err)

	// Feed 2 of 4 records — partial window stays in buffer.
	_, err = w.Process([]core.Record{
		{Payload: map[string]any{"v": 1}},
		{Payload: map[string]any{"v": 2}},
	})
	require.NoError(t, err)

	snap, err := w.Snapshot()
	require.NoError(t, err)

	// Restore into a fresh window.
	w2, err := NewTumblingWindow(4, countAgg)
	require.NoError(t, err)
	require.NoError(t, w2.Restore(snap))

	// Push 2 more records — window should fire with the 4 total.
	out, err := w2.Process([]core.Record{
		{Payload: map[string]any{"v": 3}},
		{Payload: map[string]any{"v": 4}},
	})
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, 4, out[0].Payload["count"])
}

func TestTumblingWindow_Snapshot_EmptyBuffer(t *testing.T) {
	w, _ := NewTumblingWindow(4, countAgg)
	snap, err := w.Snapshot()
	require.NoError(t, err)

	w2, _ := NewTumblingWindow(4, countAgg)
	require.NoError(t, w2.Restore(snap))
	assert.Empty(t, w2.buf)
}

func TestSlidingWindow_Snapshot_Restore(t *testing.T) {
	w, err := NewSlidingWindow(4, 2, countAgg)
	require.NoError(t, err)

	// Push 3 records: 2 trigger an emission, 1 remains partial.
	_, err = w.Process([]core.Record{
		{Payload: map[string]any{"v": 1}},
		{Payload: map[string]any{"v": 2}},
		{Payload: map[string]any{"v": 3}},
	})
	require.NoError(t, err)
	// count should be 1 after 3 records (step=2 fired once at record 2, 3rd is partial)
	assert.Equal(t, 1, w.count)

	snap, err := w.Snapshot()
	require.NoError(t, err)

	w2, err := NewSlidingWindow(4, 2, countAgg)
	require.NoError(t, err)
	require.NoError(t, w2.Restore(snap))

	assert.Equal(t, w.count, w2.count)
	assert.Len(t, w2.buf, len(w.buf))

	// One more record should trigger emission (count reaches step=2).
	out, err := w2.Process([]core.Record{{Payload: map[string]any{"v": 4}}})
	require.NoError(t, err)
	require.Len(t, out, 1)
}

func TestSlidingWindow_Snapshot_InvalidData(t *testing.T) {
	w, _ := NewSlidingWindow(4, 2, countAgg)
	err := w.Restore([]byte(`not json`))
	assert.Error(t, err)
}

func TestTumblingWindow_Snapshot_InvalidData(t *testing.T) {
	w, _ := NewTumblingWindow(4, countAgg)
	err := w.Restore([]byte(`not json`))
	assert.Error(t, err)
}
