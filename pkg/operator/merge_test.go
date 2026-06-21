package operator

import (
	"testing"

	"github.com/andrejgribov/drift/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func recN(n int) core.Record {
	return core.Record{Payload: map[string]any{"n": n}}
}

func TestMerge_HappyPath(t *testing.T) {
	extra := make(chan []core.Record, 1)
	extra <- []core.Record{recN(10), recN(11)}

	m := NewMerge(extra)
	out, err := m.Process([]core.Record{recN(1), recN(2)})
	require.NoError(t, err)
	assert.Len(t, out, 4)
	assert.Equal(t, 1, out[0].Payload["n"])
	assert.Equal(t, 2, out[1].Payload["n"])
	assert.Equal(t, 10, out[2].Payload["n"])
	assert.Equal(t, 11, out[3].Payload["n"])
}

func TestMerge_ExtraEmpty(t *testing.T) {
	extra := make(chan []core.Record, 1)
	m := NewMerge(extra)
	out, err := m.Process([]core.Record{recN(1)})
	require.NoError(t, err)
	assert.Len(t, out, 1)
}

func TestMerge_ExtraClosed(t *testing.T) {
	extra := make(chan []core.Record)
	close(extra)

	m := NewMerge(extra)
	out, err := m.Process([]core.Record{recN(1)})
	require.NoError(t, err)
	assert.Len(t, out, 1)
}

func TestMerge_EmptyPrimary(t *testing.T) {
	extra := make(chan []core.Record, 1)
	extra <- []core.Record{recN(5)}

	m := NewMerge(extra)
	out, err := m.Process(nil)
	require.NoError(t, err)
	assert.Len(t, out, 1)
	assert.Equal(t, 5, out[0].Payload["n"])
}

func TestMerge_BothEmpty(t *testing.T) {
	extra := make(chan []core.Record, 1)
	m := NewMerge(extra)
	out, err := m.Process(nil)
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestMerge_ExtraMultipleBatches(t *testing.T) {
	extra := make(chan []core.Record, 3)
	extra <- []core.Record{recN(1)}
	extra <- []core.Record{recN(2)}
	extra <- []core.Record{recN(3)}

	m := NewMerge(extra)
	out, err := m.Process(nil)
	require.NoError(t, err)
	assert.Len(t, out, 3)
}

func TestMerge_OnSchemaChange_Concurrent(t *testing.T) {
	extra := make(chan []core.Record, 10)
	m := NewMerge(extra)
	s := core.Schema{ID: "s", Version: 1}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			m.OnSchemaChange(s)
		}
	}()
	for i := 0; i < 100; i++ {
		m.Process([]core.Record{recN(i)}) //nolint:errcheck
	}
	<-done
}
