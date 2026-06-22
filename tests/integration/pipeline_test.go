package integration

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/operator"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/sink"
	"github.com/gribovan2005/drift/pkg/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeRecords(values ...int) []core.Record {
	recs := make([]core.Record, len(values))
	for i, v := range values {
		recs[i] = core.Record{Payload: map[string]any{"v": v}}
	}
	return recs
}

// TestPipeline_FilterThenMap verifies the golden path: records flow through
// two chained stages and arrive at the sink correctly.
func TestPipeline_FilterThenMap(t *testing.T) {
	src := source.NewMemory(makeRecords(1, 2, 3, 4, 5))
	snk := sink.NewMemory()

	stages := []pipeline.Stage{
		{
			Label: "filter-evens",
			Op: operator.NewFilter(func(r core.Record) bool {
				return r.Payload["v"].(int)%2 == 0
			}),
		},
		{
			Label: "double",
			Op: operator.NewMap(func(r core.Record) (core.Record, error) {
				r.Payload["v"] = r.Payload["v"].(int) * 2
				return r, nil
			}),
		},
	}

	p := pipeline.New(src, stages, snk)
	require.NoError(t, p.Run(context.Background()))

	got := snk.Records()
	require.Len(t, got, 2)
	vals := []int{got[0].Payload["v"].(int), got[1].Payload["v"].(int)}
	assert.ElementsMatch(t, []int{4, 8}, vals)
}

// TestPipeline_FlatMap expands each record into N records.
func TestPipeline_FlatMap(t *testing.T) {
	src := source.NewMemory(makeRecords(2, 3))
	snk := sink.NewMemory()

	stages := []pipeline.Stage{{
		Label: "expand",
		Op: operator.NewFlatMap(func(r core.Record) ([]core.Record, error) {
			n := r.Payload["v"].(int)
			out := make([]core.Record, n)
			for i := range out {
				out[i] = core.Record{Payload: map[string]any{"i": i}}
			}
			return out, nil
		}),
	}}

	p := pipeline.New(src, stages, snk)
	require.NoError(t, p.Run(context.Background()))

	// 2 + 3 = 5 records total
	assert.Len(t, snk.Records(), 5)
}

// TestPipeline_LiveSchemaEvolution verifies that SchemaAdapter handles
// old-schema records without stopping the pipeline.
func TestPipeline_LiveSchemaEvolution(t *testing.T) {
	v1 := core.Schema{
		ID: "ev", Version: 1,
		Fields: []core.Field{{Name: "id", Type: core.FieldTypeString}, {Name: "val", Type: core.FieldTypeFloat}},
	}
	v2 := core.Schema{
		ID: "ev", Version: 2,
		Fields: []core.Field{
			{Name: "id", Type: core.FieldTypeString},
			{Name: "score", Type: core.FieldTypeFloat, Default: 0.0}, // renamed from val
			{Name: "env", Type: core.FieldTypeString, Default: "prod"},
		},
	}

	// Mix of v1 and v2-era records (both sent pre-evolution for simplicity).
	recs := []core.Record{
		{SchemaID: "ev", SchemaVersion: 1, Payload: map[string]any{"id": "a", "val": 1.5}},
		{SchemaID: "ev", SchemaVersion: 1, Payload: map[string]any{"id": "b", "val": 2.5}},
	}

	src := source.NewMemory(recs)
	snk := sink.NewMemory()

	aliases := operator.AliasMap{"val": "score"}
	adapter := operator.NewSchemaAdapter(v1, aliases)

	// Evolve schema before processing (simulates live change arriving before records).
	adapter.OnSchemaChange(v2)

	stages := []pipeline.Stage{{Label: "adapt", Op: adapter}}
	p := pipeline.New(src, stages, snk)
	require.NoError(t, p.Run(context.Background()))

	got := snk.Records()
	require.Len(t, got, 2)
	for _, r := range got {
		assert.Equal(t, 2, r.SchemaVersion)
		assert.Equal(t, "prod", r.Payload["env"])
		score, hasScore := r.Payload["score"]
		assert.True(t, hasScore)
		assert.NotNil(t, score)
	}
}

// TestPipeline_EmptySource ensures the pipeline completes cleanly with no records.
func TestPipeline_EmptySource(t *testing.T) {
	src := source.NewMemory(nil)
	snk := sink.NewMemory()
	p := pipeline.New(src, nil, snk)
	require.NoError(t, p.Run(context.Background()))
	assert.Empty(t, snk.Records())
}
