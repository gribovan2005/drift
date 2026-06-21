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

// TestPipeline_DAG_FanOut verifies that one stage can broadcast to two
// downstream stages (fan-out). Both sinks should receive all records.
func TestPipeline_DAG_FanOut(t *testing.T) {
	const n = 10
	records := makeRecords(n)

	sinkA := sink.NewMemory()
	sinkB := sink.NewMemory()

	// Use a channel to capture the split stream for sinkB.
	// We route even v to "b", odd v to the primary sink via Split.
	sp, err := operator.NewSplit(2, func(r core.Record) int {
		if r.Payload["v"].(int)%2 == 0 {
			return 1 // side channel
		}
		return 0 // primary
	}, 100)
	require.NoError(t, err)

	// Drain side channel into sinkB concurrently.
	sideDone := make(chan struct{})
	go func() {
		defer close(sideDone)
		for r := range sp.Outputs()[0] {
			sinkB.Write(context.Background(), func() <-chan core.Record { //nolint:staticcheck
				ch := make(chan core.Record, 1)
				ch <- r
				close(ch)
				return ch
			}())
		}
	}()

	src := source.NewMemory(records)
	p := New(src, []Stage{{Label: "split", Op: sp}}, sinkA)
	require.NoError(t, p.Run(context.Background()))
	sp.Close()
	<-sideDone

	// Primary (odd): 1,3,5,7,9
	assert.Len(t, sinkA.Records(), 5)
	// Side (even): 0,2,4,6,8
	assert.Len(t, sinkB.Records(), 5)
}

// TestPipeline_DAG_Diamond verifies source→A→[B,C]→D→sink topology.
// A broadcasts to B and C; both feed D. The sink receives 2n records.
//
// Fan-out shares the Payload map reference — operators must create new records
// instead of mutating the input in-place (standard Map contract).
func TestPipeline_DAG_Diamond(t *testing.T) {
	const n = 20
	records := makeRecords(n)

	// A: doubles v
	opA := operator.NewMap(func(r core.Record) (core.Record, error) {
		return core.Record{Payload: map[string]any{"v": r.Payload["v"].(int), "a": true}}, nil
	})
	// B: tags path="B" — new record, no mutation of shared payload
	opB := operator.NewMap(func(r core.Record) (core.Record, error) {
		return core.Record{Payload: map[string]any{"v": r.Payload["v"], "path": "B"}}, nil
	})
	// C: tags path="C"
	opC := operator.NewMap(func(r core.Record) (core.Record, error) {
		return core.Record{Payload: map[string]any{"v": r.Payload["v"], "path": "C"}}, nil
	})
	// D: identity
	opD := operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil })

	stages := []Stage{
		{Label: "A", Op: opA, Next: []string{"B", "C"}},
		{Label: "B", Op: opB, Next: []string{"D"}},
		{Label: "C", Op: opC, Next: []string{"D"}},
		{Label: "D", Op: opD},
	}

	src := source.NewMemory(records)
	snk := sink.NewMemory()
	p := New(src, stages, snk)
	require.NoError(t, p.Run(context.Background()))

	got := snk.Records()
	// Each of n records is broadcast to B and C → D receives 2n records.
	assert.Len(t, got, n*2)

	bCount, cCount := 0, 0
	for _, r := range got {
		switch r.Payload["path"] {
		case "B":
			bCount++
		case "C":
			cCount++
		}
	}
	assert.Equal(t, n, bCount, "B should have processed n records")
	assert.Equal(t, n, cCount, "C should have processed n records")
}

// TestPipeline_Graph_DAG verifies that explicit Next wiring is reflected in Graph().
func TestPipeline_Graph_DAG(t *testing.T) {
	noop := operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil })
	stages := []Stage{
		{Label: "A", Op: noop, Next: []string{"B", "C"}},
		{Label: "B", Op: noop, Next: []string{"D"}},
		{Label: "C", Op: noop, Next: []string{"D"}},
		{Label: "D", Op: noop},
	}
	p := New(source.NewMemory(nil), stages, sink.NewMemory())
	graph := p.Graph()

	require.Len(t, graph, 4)
	assert.Equal(t, []string{"B", "C"}, graph[0].Next)
	assert.Equal(t, []string{"D"}, graph[1].Next)
	assert.Equal(t, []string{"D"}, graph[2].Next)
	assert.Empty(t, graph[3].Next)
}
