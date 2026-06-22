package job

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/sink"
	"github.com/gribovan2005/drift/pkg/source"

	"github.com/stretchr/testify/require"
)

// TestGoldenPath_ColumnarGroupBy runs the declarative fast-lane end to end: row source
// → to-batch (row→columnar) → vec-groupby → to-rows (columnar→row) → row sink, all via
// the YAML operator registry. Proves the fast analytical path is reachable without Go.
func TestGoldenPath_ColumnarGroupBy(t *testing.T) {
	mk := func(op string, params map[string]any) core.Operator {
		o, err := buildOperator(StageSpec{Op: op, Params: params})
		require.NoError(t, err)
		return o
	}
	rows := []core.Record{
		{Payload: map[string]any{"merchant": "a", "amount": 1.0}},
		{Payload: map[string]any{"merchant": "b", "amount": 2.0}},
		{Payload: map[string]any{"merchant": "a", "amount": 3.0}},
		{Payload: map[string]any{"merchant": "a", "amount": 4.0}},
		{Payload: map[string]any{"merchant": "b", "amount": 5.0}},
	}
	out := sink.NewMemory()
	p := pipeline.New(
		source.NewMemory(rows),
		[]pipeline.Stage{
			{Label: "batch", Op: mk("to-batch", map[string]any{"size": 2})}, // forces 2+2+1 chunks
			{Label: "agg", Op: mk("vec-groupby", map[string]any{"key": "merchant", "agg": "count, sum:amount"})},
			{Label: "rows", Op: mk("to-rows", nil)},
		},
		out,
	)
	require.NoError(t, p.Run(context.Background()))

	got := map[string][2]float64{} // merchant → {count, sum_amount}
	for _, r := range out.Records() {
		m := r.Payload["merchant"].(string)
		got[m] = [2]float64{float64(r.Payload["count"].(int64)), r.Payload["sum_amount"].(float64)}
	}
	require.Equal(t, map[string][2]float64{
		"a": {3, 8.0},
		"b": {2, 7.0},
	}, got)
}

// TestGoldenPath_LoadsFromYAML proves the whole pipeline parses and builds from YAML.
func TestGoldenPath_LoadsFromYAML(t *testing.T) {
	yaml := []byte(`
name: fastlane-demo
source:
  type: generator
  rate: "1ms"
  fields:
    merchant: "choice:a|b"
    amount: "rand:int:1:9"
stages:
  - label: batch
    op: to-batch
    size: 256
  - label: agg
    op: vec-groupby
    key: merchant
    agg: "count, sumi:amount, max:amount"
  - label: rows
    op: to-rows
sink:
  type: memory
`)
	built, err := Load(yaml)
	require.NoError(t, err)
	require.Len(t, built.Stages, 3)
	require.Equal(t, "agg", built.Stages[1].Label)
}

// TestVecOp_RejectsParallelism: fast-lane ops are single-stage.
func TestVecOp_RejectsParallelism(t *testing.T) {
	_, err := buildStageOp(StageSpec{Label: "g", Op: "vec-groupby", Parallelism: 2,
		Params: map[string]any{"key": "merchant"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "single-stage")
}
