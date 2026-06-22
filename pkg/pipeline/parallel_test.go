package pipeline

import (
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/operator"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func parRecs(vals ...int) []core.Record {
	out := make([]core.Record, len(vals))
	for i, v := range vals {
		out[i] = core.Record{Payload: map[string]any{"v": v}}
	}
	return out
}

// TestParallel_StatelessRoundRobin: all records processed across shards (multiset
// preserved), values transformed.
func TestParallel_StatelessRoundRobin(t *testing.T) {
	mk := func() core.Operator {
		return operator.NewMap(func(r core.Record) (core.Record, error) {
			r.Payload = map[string]any{"v": r.Payload["v"].(int) * 10}
			return r, nil
		})
	}
	p := Parallel([]core.Operator{mk(), mk(), mk()}, nil)

	out, err := p.Process(parRecs(1, 2, 3, 4, 5))
	require.NoError(t, err)
	require.Len(t, out, 5)

	got := map[int]bool{}
	for _, r := range out {
		got[r.Payload["v"].(int)] = true
	}
	assert.Equal(t, map[int]bool{10: true, 20: true, 30: true, 40: true, 50: true}, got)
}

// TestParallel_KeyedDedup: routing by key keeps a keyed stateful op correct —
// duplicates always land on the same shard, so they are caught.
func TestParallel_KeyedDedup(t *testing.T) {
	key := func(r core.Record) string { return r.Payload["k"].(string) }
	mk := func() core.Operator { return operator.NewDeduplicate(key, time.Minute) }
	p := Parallel([]core.Operator{mk(), mk()}, key)

	in := []core.Record{
		{Payload: map[string]any{"k": "a"}},
		{Payload: map[string]any{"k": "b"}},
		{Payload: map[string]any{"k": "a"}}, // dup
		{Payload: map[string]any{"k": "c"}},
		{Payload: map[string]any{"k": "b"}}, // dup
	}
	out, err := p.Process(in)
	require.NoError(t, err)
	assert.Len(t, out, 3, "duplicates must be removed regardless of sharding")
}

// TestParallel_PreservesSnapshot: a parallelized Snapshottable op (dedup)
// round-trips state through Snapshot/Restore across shards.
func TestParallel_PreservesSnapshot(t *testing.T) {
	key := func(r core.Record) string { return r.Payload["k"].(string) }
	mk := func() core.Operator { return operator.NewDeduplicate(key, time.Minute) }
	p := Parallel([]core.Operator{mk(), mk()}, key)

	sn, isS := p.(core.Snapshottable)
	require.True(t, isS)

	_, err := p.Process([]core.Record{{Payload: map[string]any{"k": "a"}}, {Payload: map[string]any{"k": "b"}}})
	require.NoError(t, err)
	blob, err := sn.Snapshot()
	require.NoError(t, err)

	p2 := Parallel([]core.Operator{mk(), mk()}, key)
	require.NoError(t, p2.(core.Snapshottable).Restore(blob))

	// "a" is already seen → dropped after restore (routed to the same shard).
	out, err := p2.Process([]core.Record{{Payload: map[string]any{"k": "a"}}})
	require.NoError(t, err)
	assert.Empty(t, out)
}

// TestParallel_PreservesFlusher: a parallelized Flusher (tumbling) exposes Flush,
// which drains all shards.
func TestParallel_PreservesFlusher(t *testing.T) {
	mk := func() core.Operator {
		w, err := operator.NewTumblingWindow(100, func(rs []core.Record) (core.Record, error) {
			return core.Record{Payload: map[string]any{"count": len(rs)}}, nil
		})
		require.NoError(t, err)
		return w
	}
	p := Parallel([]core.Operator{mk(), mk()}, nil)

	f, isF := p.(core.Flusher)
	require.True(t, isF)

	// Windows of 100 never close from 6 records; Flush emits the partial windows.
	_, err := p.Process(parRecs(1, 2, 3, 4, 5, 6))
	require.NoError(t, err)
	flushed, err := f.Flush()
	require.NoError(t, err)
	total := 0
	for _, r := range flushed {
		total += r.Payload["count"].(int)
	}
	assert.Equal(t, 6, total, "flush across shards must account for all buffered records")
}

func TestParallel_Empty(t *testing.T) {
	p := Parallel([]core.Operator{operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil })}, nil)
	out, err := p.Process(nil)
	require.NoError(t, err)
	assert.Empty(t, out)
}
