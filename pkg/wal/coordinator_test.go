package wal_test

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/pkg/checkpoint"
	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/sink"
	"github.com/gribovan2005/drift/pkg/source"
	"github.com/gribovan2005/drift/pkg/wal"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeRecs(vals ...int) []core.Record {
	out := make([]core.Record, len(vals))
	for i, v := range vals {
		out[i] = core.Record{SchemaID: "events", SchemaVersion: 1, Payload: map[string]any{"v": v}}
	}
	return out
}

// drain reads a source channel to completion.
func drain(t *testing.T, ch <-chan core.Record) []core.Record {
	t.Helper()
	var out []core.Record
	for r := range ch {
		out = append(out, r)
	}
	return out
}

// feed pushes recs through an idempotent sink and returns when it finishes.
func feed(t *testing.T, s core.Sink, recs []core.Record) {
	t.Helper()
	ch := make(chan core.Record, len(recs))
	for _, r := range recs {
		ch <- r
	}
	close(ch)
	require.NoError(t, s.Write(context.Background(), ch))
}

func TestCoordinator_NoDuplicatesOnReplay(t *testing.T) {
	dir := t.TempDir()
	log, err := wal.Open(dir + "/wal")
	require.NoError(t, err)
	defer log.Close()
	seen, err := checkpoint.NewFileStore(dir + "/seen")
	require.NoError(t, err)
	coord := wal.NewCoordinator(log, seen)

	src := coord.Source(source.NewMemory(makeRecs(1, 2, 3)))
	ch, err := src.Read(context.Background())
	require.NoError(t, err)
	emitted := drain(t, ch)
	require.Len(t, emitted, 3)
	for _, r := range emitted {
		assert.NotEmpty(t, r.DeliveryKey)
	}

	// First delivery: all three land.
	out1 := sink.NewMemory()
	feed(t, coord.Sink(out1), emitted)
	require.Len(t, out1.Records(), 3)

	// Replay the exact same records (same keys): all are duplicates → skipped.
	out2 := sink.NewMemory()
	feed(t, coord.Sink(out2), emitted)
	assert.Empty(t, out2.Records())
}

func TestCoordinator_NoLossOnCrash(t *testing.T) {
	dir := t.TempDir()
	log, err := wal.Open(dir + "/wal")
	require.NoError(t, err)
	seen, err := checkpoint.NewFileStore(dir + "/seen")
	require.NoError(t, err)
	coord := wal.NewCoordinator(log, seen)

	// Run 1: source appends 3 records to the WAL; the sink durably writes only
	// the first 2 before a "crash".
	ch, err := coord.Source(source.NewMemory(makeRecs(10, 20, 30))).Read(context.Background())
	require.NoError(t, err)
	emitted := drain(t, ch)
	require.Len(t, emitted, 3)

	out1 := sink.NewMemory()
	feed(t, coord.Sink(out1), emitted[:2])
	require.Len(t, out1.Records(), 2)
	require.NoError(t, log.Close())

	// Restart: reopen the log; the un-committed 3rd record must be replayed.
	log2, err := wal.Open(dir + "/wal")
	require.NoError(t, err)
	defer log2.Close()
	coord2 := wal.NewCoordinator(log2, seen)

	ch2, err := coord2.Source(source.NewMemory(nil)).Read(context.Background())
	require.NoError(t, err)
	replayed := drain(t, ch2)
	require.Len(t, replayed, 1)
	// Replayed records round-trip through JSON in the WAL, so numbers decode as
	// float64 — assert on the JSON-native value.
	assert.EqualValues(t, 30, replayed[0].Payload["v"])

	out2 := sink.NewMemory()
	feed(t, coord2.Sink(out2), replayed)
	require.Len(t, out2.Records(), 1)

	// Union across both runs: every record delivered exactly once, none lost.
	assert.Equal(t, 3, len(out1.Records())+len(out2.Records()))
}

func TestCoordinator_CommitAdvancesOnAck(t *testing.T) {
	dir := t.TempDir()
	log, err := wal.Open(dir + "/wal")
	require.NoError(t, err)
	defer log.Close()
	seen, err := checkpoint.NewFileStore(dir + "/seen")
	require.NoError(t, err)
	coord := wal.NewCoordinator(log, seen)

	ch, err := coord.Source(source.NewMemory(makeRecs(1, 2, 3))).Read(context.Background())
	require.NoError(t, err)
	emitted := drain(t, ch)

	// Appended but not yet acked by any sink.
	assert.Equal(t, uint64(0), log.Committed())

	feed(t, coord.Sink(sink.NewMemory()), emitted)

	// Watermark advanced to the last LSN only after the sink wrote them.
	assert.Equal(t, uint64(3), log.Committed())
}
