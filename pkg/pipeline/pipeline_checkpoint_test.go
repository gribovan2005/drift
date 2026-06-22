package pipeline

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/pkg/checkpoint"
	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/operator"
	"github.com/gribovan2005/drift/pkg/sink"
	"github.com/gribovan2005/drift/pkg/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sumAgg(recs []core.Record) (core.Record, error) {
	sum := 0
	for _, r := range recs {
		if v, ok := r.Payload["v"].(float64); ok {
			sum += int(v)
		}
	}
	return core.Record{Payload: map[string]any{"sum": sum}}, nil
}

// TestPipeline_CheckpointSaveRestore simulates a crash mid-stream.
// On a clean shutdown the pipeline calls Flush() before saving, so the
// window buffer is empty. Crash recovery is tested by manually snapshotting
// the window (bypassing pipeline) and then letting the pipeline restore it.
func TestPipeline_CheckpointSaveRestore(t *testing.T) {
	store, err := checkpoint.NewFileStore(t.TempDir())
	require.NoError(t, err)

	// Simulate: window processed 2 of 4 records before crash.
	win, err := operator.NewTumblingWindow(4, sumAgg)
	require.NoError(t, err)
	_, err = win.Process([]core.Record{
		{Payload: map[string]any{"v": float64(10)}},
		{Payload: map[string]any{"v": float64(20)}},
	})
	require.NoError(t, err)

	// Crash: save checkpoint directly (pipeline would do this on shutdown).
	data, err := win.Snapshot()
	require.NoError(t, err)
	require.NoError(t, store.Save("win", data))

	// Restart: pipeline restores window state and processes 2 more records.
	win2, err := operator.NewTumblingWindow(4, sumAgg)
	require.NoError(t, err)

	snk := sink.NewMemory()
	p := New(
		source.NewMemory([]core.Record{
			{Payload: map[string]any{"v": float64(30)}},
			{Payload: map[string]any{"v": float64(40)}},
		}),
		[]Stage{{Label: "win", Op: win2}},
		snk,
		WithCheckpoint(store),
	)
	require.NoError(t, p.Run(context.Background()))

	// Window fires when all 4 records are present: 10+20+30+40 = 100.
	require.Len(t, snk.Records(), 1)
	assert.EqualValues(t, 100, snk.Records()[0].Payload["sum"])
}

func TestPipeline_CheckpointRestore_BadData(t *testing.T) {
	store, err := checkpoint.NewFileStore(t.TempDir())
	require.NoError(t, err)

	// Corrupt checkpoint — pipeline must not fail, just log a warning.
	require.NoError(t, store.Save("win", []byte(`not json`)))

	win, err := operator.NewTumblingWindow(2, sumAgg)
	require.NoError(t, err)

	p := New(
		source.NewMemory([]core.Record{{Payload: map[string]any{"v": float64(1)}}}),
		[]Stage{{Label: "win", Op: win}},
		sink.NewMemory(),
		WithCheckpoint(store),
	)
	assert.NoError(t, p.Run(context.Background()))
}

func TestPipeline_IsReady(t *testing.T) {
	records := []core.Record{{Payload: map[string]any{"v": 1}}}
	snk := sink.NewMemory()
	p := New(
		source.NewMemory(records),
		[]Stage{{Label: "noop", Op: operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil })}},
		snk,
	)
	assert.False(t, p.IsReady(), "not ready before run")
	require.NoError(t, p.Run(context.Background()))
	assert.True(t, p.IsReady(), "ready after records processed")
}

func TestPipeline_CheckpointSaved_OnCleanShutdown(t *testing.T) {
	store, err := checkpoint.NewFileStore(t.TempDir())
	require.NoError(t, err)

	win, err := operator.NewTumblingWindow(10, sumAgg)
	require.NoError(t, err)

	// Run pipeline — window does not fire (only 1 of 10 records). Flush emits partial.
	p := New(
		source.NewMemory([]core.Record{{Payload: map[string]any{"v": float64(1)}}}),
		[]Stage{{Label: "win", Op: win}},
		sink.NewMemory(),
		WithCheckpoint(store),
	)
	require.NoError(t, p.Run(context.Background()))

	// After clean shutdown, checkpoint file must exist (empty buf after flush).
	_, found, err := store.Load("win")
	require.NoError(t, err)
	assert.True(t, found, "checkpoint must be written on clean shutdown")
}
