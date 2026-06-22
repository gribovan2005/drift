package integration

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/lineage"
	"github.com/gribovan2005/drift/pkg/operator"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/sink"
	"github.com/gribovan2005/drift/pkg/source"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPipeline_Lineage_EndToEnd runs a real two-stage pipeline with lineage
// enabled and verifies every sink record traces back to a source root.
func TestPipeline_Lineage_EndToEnd(t *testing.T) {
	in := []core.Record{
		{SchemaID: "events", SchemaVersion: 1, Payload: map[string]any{"v": 2}},
		{SchemaID: "events", SchemaVersion: 1, Payload: map[string]any{"v": 7}},
		{SchemaID: "events", SchemaVersion: 1, Payload: map[string]any{"v": 12}},
	}
	src := source.NewMemory(in)
	snk := sink.NewMemory()

	stages := []pipeline.Stage{
		{
			Label: "keep-big",
			Op: operator.NewFilter(func(r core.Record) bool {
				return r.Payload["v"].(int) >= 5
			}),
			Next: []string{"tag"},
		},
		{
			Label: "tag",
			Op: operator.NewMap(func(r core.Record) (core.Record, error) {
				r.Payload["flagged"] = true
				return r, nil
			}),
		},
	}

	tr := lineage.New()
	p := pipeline.New(src, stages, snk, pipeline.WithLineage(tr))
	require.NoError(t, p.Run(context.Background()))

	got := snk.Records()
	require.Len(t, got, 2) // 7 and 12 survive the filter

	for _, r := range got {
		require.NotEmpty(t, r.ID, "sink record must carry a lineage ID")

		node, ok := tr.Get(r.ID)
		require.True(t, ok)
		assert.Equal(t, "tag", node.Stage)

		// Walk back to the source: must reach exactly one root.
		roots := tr.Roots(r.ID)
		require.Len(t, roots, 1)
		assert.Equal(t, "source", roots[0].Stage)
		assert.Equal(t, "events", roots[0].SchemaID)

		// Ancestry: tag-output ← filter-output ← source root = 2 ancestors.
		anc := tr.Ancestors(r.ID)
		assert.Len(t, anc, 2)
	}

	// Three roots ingested, two survived each producing one filter + one tag node.
	assert.Equal(t, 3+2+2, tr.Len())
}
