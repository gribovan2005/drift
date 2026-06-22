package operator

import (
	"testing"

	"github.com/gribovan2005/drift/pkg/core"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func valRec(v int) core.Record { return core.Record{Payload: map[string]any{"v": v}} }

func byV(r core.Record) float64 { return float64(r.Payload["v"].(int)) }

func TestTopN_GlobalTopByValue(t *testing.T) {
	top, err := NewTopN(nil, byV, 2, 5)
	require.NoError(t, err)

	// Window of 5 → top-2 by value: 9, 7.
	out, err := top.Process([]core.Record{valRec(3), valRec(9), valRec(1), valRec(7), valRec(5)})
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, 9, out[0].Payload["v"])
	assert.Equal(t, 1, out[0].Payload["rank"])
	assert.Equal(t, 7, out[1].Payload["v"])
	assert.Equal(t, 2, out[1].Payload["rank"])
}

func TestTopN_PerKey(t *testing.T) {
	key := func(r core.Record) string { return r.Payload["k"].(string) }
	by := func(r core.Record) float64 { return float64(r.Payload["v"].(int)) }
	top, err := NewTopN(key, by, 1, 2)
	require.NoError(t, err)

	in := []core.Record{
		{Payload: map[string]any{"k": "a", "v": 1}},
		{Payload: map[string]any{"k": "a", "v": 9}}, // a fills → top-1 = 9
		{Payload: map[string]any{"k": "b", "v": 4}},
		{Payload: map[string]any{"k": "b", "v": 2}}, // b fills → top-1 = 4
	}
	out, err := top.Process(in)
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, 9, out[0].Payload["v"])
	assert.Equal(t, 4, out[1].Payload["v"])
}

func TestTopN_FlushPartial(t *testing.T) {
	top, _ := NewTopN(nil, byV, 3, 100)
	_, err := top.Process([]core.Record{valRec(5), valRec(8)})
	require.NoError(t, err)
	out, err := top.Flush()
	require.NoError(t, err)
	require.Len(t, out, 2) // only 2 buffered; top-3 capped to 2
	assert.Equal(t, 8, out[0].Payload["v"])
}

func TestTopN_Bad(t *testing.T) {
	_, err := NewTopN(nil, byV, 0, 5)
	require.Error(t, err)
}
