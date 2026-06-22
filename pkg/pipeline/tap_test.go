package pipeline

import (
	"testing"

	"github.com/gribovan2005/drift/pkg/core"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tapRecs(vals ...int) []core.Record {
	out := make([]core.Record, len(vals))
	for i, v := range vals {
		out[i] = core.Record{Payload: map[string]any{"v": v}}
	}
	return out
}

func TestTap_RecordAndSampleRing(t *testing.T) {
	tap := NewTap(3)
	tap.record("s", tapRecs(1, 2))
	tap.record("s", tapRecs(3, 4, 5))

	got := tap.Sample("s")
	require.Len(t, got, 3) // trimmed to last 3
	assert.Equal(t, 3, got[0].Payload["v"])
	assert.Equal(t, 5, got[2].Payload["v"])

	assert.Empty(t, tap.Sample("missing"))
}

func TestTap_NilSafe(t *testing.T) {
	var tap *Tap
	tap.record("s", tapRecs(1)) // must not panic
}
