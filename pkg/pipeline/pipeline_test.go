package pipeline

import (
	"context"
	"testing"

	"github.com/andrejgribov/drift/pkg/core"
	"github.com/andrejgribov/drift/pkg/operator"
	"github.com/andrejgribov/drift/pkg/sink"
	"github.com/andrejgribov/drift/pkg/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeRecords(n int) []core.Record {
	recs := make([]core.Record, n)
	for i := range recs {
		recs[i] = core.Record{Payload: map[string]any{"v": i}}
	}
	return recs
}

func TestPipeline_MetricsCollected(t *testing.T) {
	records := makeRecords(200)
	src := source.NewMemory(records)
	snk := sink.NewMemory()

	stages := []Stage{
		{
			Label: "double",
			Op: operator.NewMap(func(r core.Record) (core.Record, error) {
				r.Payload["v"] = r.Payload["v"].(int) * 2
				return r, nil
			}),
		},
	}

	p := New(src, stages, snk)
	require.NoError(t, p.Run(context.Background()))

	snap := p.Snapshot()
	require.Len(t, snap.Stages, 1)
	assert.Equal(t, "double", snap.Stages[0].Label)
	assert.Equal(t, int64(200), snap.Stages[0].ProcessedTotal)
	assert.Zero(t, snap.Stages[0].ErrorTotal)
}

func TestPipeline_Graph_Linear(t *testing.T) {
	src := source.NewMemory(nil)
	snk := sink.NewMemory()
	stages := []Stage{
		{Label: "a", Op: operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil })},
		{Label: "b", Op: operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil })},
		{Label: "c", Op: operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil })},
	}
	p := New(src, stages, snk)

	graph := p.Graph()
	require.Len(t, graph, 3)
	assert.Equal(t, []string{"b"}, graph[0].Next)
	assert.Equal(t, []string{"c"}, graph[1].Next)
	assert.Empty(t, graph[2].Next)
}
