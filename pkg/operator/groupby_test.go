package operator

import (
	"testing"

	"github.com/andrejgribov/drift/pkg/core"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func gbRec(k string) core.Record { return core.Record{Payload: map[string]any{"k": k}} }

func keyK(r core.Record) string { return r.Payload["k"].(string) }

func TestKeyedCountWindow_FiresPerKey(t *testing.T) {
	w, err := NewKeyedCountWindow(keyK, 3, CountAgg("k"))
	require.NoError(t, err)

	// a a b a (a hits 3 → fires count=3; b has 1 buffered)
	out, err := w.Process([]core.Record{gbRec("a"), gbRec("a"), gbRec("b"), gbRec("a")})
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, "a", out[0].Payload["k"])
	assert.Equal(t, 3, out[0].Payload["count"])

	// Flush emits partial windows per key in key order (b:1).
	flushed, err := w.Flush()
	require.NoError(t, err)
	require.Len(t, flushed, 1)
	assert.Equal(t, "b", flushed[0].Payload["k"])
	assert.Equal(t, 1, flushed[0].Payload["count"])
}

func TestKeyedCountWindow_SnapshotRestore(t *testing.T) {
	w, _ := NewKeyedCountWindow(keyK, 5, CountAgg("k"))
	_, err := w.Process([]core.Record{gbRec("a"), gbRec("a"), gbRec("b")})
	require.NoError(t, err)

	blob, err := w.Snapshot()
	require.NoError(t, err)

	w2, _ := NewKeyedCountWindow(keyK, 5, CountAgg("k"))
	require.NoError(t, w2.Restore(blob))

	// "a" already has 2 buffered; 3 more → fires count=5.
	out, err := w2.Process([]core.Record{gbRec("a"), gbRec("a"), gbRec("a")})
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, 5, out[0].Payload["count"])
}

func TestKeyedCountWindow_BadSize(t *testing.T) {
	_, err := NewKeyedCountWindow(keyK, 0, CountAgg("k"))
	require.Error(t, err)
}
