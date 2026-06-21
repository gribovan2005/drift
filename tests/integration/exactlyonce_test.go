package integration

import (
	"context"
	"testing"

	"github.com/andrejgribov/drift/pkg/checkpoint"
	"github.com/andrejgribov/drift/pkg/core"
	"github.com/andrejgribov/drift/pkg/operator"
	"github.com/andrejgribov/drift/pkg/pipeline"
	"github.com/andrejgribov/drift/pkg/sink"
	"github.com/andrejgribov/drift/pkg/source"
	"github.com/andrejgribov/drift/pkg/wal"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPipeline_ExactlyOnce_CrashReplay drives a real pipeline through a crash and
// two restarts, proving the WAL source + idempotent sink deliver each record
// exactly once: no loss on recovery, no duplicates on a subsequent restart.
func TestPipeline_ExactlyOnce_CrashReplay(t *testing.T) {
	dir := t.TempDir()
	walDir, seenDir := dir+"/wal", dir+"/seen"

	in := []core.Record{
		{SchemaID: "events", SchemaVersion: 1, Payload: map[string]any{"v": 1}},
		{SchemaID: "events", SchemaVersion: 1, Payload: map[string]any{"v": 2}},
		{SchemaID: "events", SchemaVersion: 1, Payload: map[string]any{"v": 3}},
		{SchemaID: "events", SchemaVersion: 1, Payload: map[string]any{"v": 4}},
	}

	tag := func() pipeline.Stage {
		return pipeline.Stage{Label: "tag", Op: operator.NewMap(func(r core.Record) (core.Record, error) {
			r.Payload["seen"] = true
			return r, nil
		})}
	}

	// ── Run 1 (crash): records enter the WAL but the sink never commits. ──
	log1, err := wal.Open(walDir)
	require.NoError(t, err)
	seen, err := checkpoint.NewFileStore(seenDir)
	require.NoError(t, err)
	coord1 := wal.NewCoordinator(log1, seen)

	ch, err := coord1.Source(source.NewMemory(in)).Read(context.Background())
	require.NoError(t, err)
	for range ch { // drain: all 4 appended to the WAL, nothing acked
	}
	require.Equal(t, uint64(0), log1.Committed())
	require.NoError(t, log1.Close())

	// ── Run 2 (recovery): a real pipeline replays the un-committed records. ──
	log2, err := wal.Open(walDir)
	require.NoError(t, err)
	coord2 := wal.NewCoordinator(log2, seen)
	out2 := sink.NewMemory()
	p2 := pipeline.New(
		coord2.Source(source.NewMemory(nil)), // empty upstream → replay only
		[]pipeline.Stage{tag()},
		coord2.Sink(out2),
	)
	require.NoError(t, p2.Run(context.Background()))
	require.NoError(t, log2.Close())

	// No loss: every original record recovered exactly once.
	require.Len(t, out2.Records(), 4)
	got := map[float64]int{}
	for _, r := range out2.Records() {
		assert.Equal(t, true, r.Payload["seen"])
		got[r.Payload["v"].(float64)]++
	}
	assert.Equal(t, map[float64]int{1: 1, 2: 1, 3: 1, 4: 1}, got)

	// ── Run 3 (clean restart): everything is committed → nothing replays. ──
	log3, err := wal.Open(walDir)
	require.NoError(t, err)
	defer log3.Close()
	assert.Equal(t, uint64(4), log3.Committed())
	coord3 := wal.NewCoordinator(log3, seen)
	out3 := sink.NewMemory()
	p3 := pipeline.New(
		coord3.Source(source.NewMemory(nil)),
		[]pipeline.Stage{tag()},
		coord3.Sink(out3),
	)
	require.NoError(t, p3.Run(context.Background()))

	// No duplicates: the recovered records are not delivered again.
	assert.Empty(t, out3.Records())
}
