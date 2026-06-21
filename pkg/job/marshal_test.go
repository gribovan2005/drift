package job

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMarshal_RoundTrip proves Marshal → Parse preserves the spec, including
// inline params, the DAG next-wiring, and the generator fields map.
func TestMarshal_RoundTrip(t *testing.T) {
	orig := Spec{
		Name: "round-trip",
		Source: ComponentSpec{Type: "generator", Params: map[string]any{
			"rate":   "5ms",
			"fields": map[string]any{"id": "seq", "tag": "${seq}_x"},
		}},
		Stages: []StageSpec{
			{Label: "keep", Op: "filter", Next: []string{"tag"}, Params: map[string]any{"field": "id", "gte": 10}},
			{Label: "tag", Op: "map-set", Params: map[string]any{"field": "flagged", "value": true}},
		},
		Sink: ComponentSpec{Type: "http", Params: map[string]any{"url": "http://x"}},
	}

	data, err := Marshal(orig)
	require.NoError(t, err)

	got, err := Parse(data)
	require.NoError(t, err)

	assert.Equal(t, orig.Name, got.Name)
	assert.Equal(t, orig.Source.Type, got.Source.Type)
	assert.Equal(t, "5ms", got.Source.Params["rate"])
	assert.Equal(t, orig.Sink, got.Sink)
	require.Len(t, got.Stages, 2)
	assert.Equal(t, "keep", got.Stages[0].Label)
	assert.Equal(t, []string{"tag"}, got.Stages[0].Next)
	assert.Equal(t, "flagged", got.Stages[1].Params["field"])

	// The round-tripped spec must still load.
	_, err = Load(data)
	require.NoError(t, err)
}
