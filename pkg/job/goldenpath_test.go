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

// TestGoldenPath_ColumnarMapFilter runs vec-map (×10) then vec-filter (≥25) declaratively.
func TestGoldenPath_ColumnarMapFilter(t *testing.T) {
	mk := func(op string, params map[string]any) core.Operator {
		o, err := buildOperator(StageSpec{Op: op, Params: params})
		require.NoError(t, err)
		return o
	}
	rows := make([]core.Record, 5)
	for i := range rows {
		rows[i] = core.Record{Payload: map[string]any{"v": int64(i)}} // 0..4 → ×10 → 0,10,20,30,40
	}
	out := sink.NewMemory()
	p := pipeline.New(source.NewMemory(rows), []pipeline.Stage{
		{Label: "batch", Op: mk("to-batch", map[string]any{"size": 8})},
		{Label: "x10", Op: mk("vec-map", map[string]any{"field": "v", "arith": "mul", "value": 10})},
		{Label: "ge25", Op: mk("vec-filter", map[string]any{"field": "v", "cmp": "gte", "value": 25})},
		{Label: "rows", Op: mk("to-rows", nil)},
	}, out)
	require.NoError(t, p.Run(context.Background()))

	var got []int64
	for _, r := range out.Records() {
		got = append(got, r.Payload["v"].(int64))
	}
	require.ElementsMatch(t, []int64{30, 40}, got) // 0,10,20 dropped
}

// TestGoldenPath_ColumnarJoin enriches a probe stream from an inline dimension table.
func TestGoldenPath_ColumnarJoin(t *testing.T) {
	mk := func(op string, params map[string]any) core.Operator {
		o, err := buildOperator(StageSpec{Op: op, Params: params})
		require.NoError(t, err)
		return o
	}
	rows := []core.Record{
		{Payload: map[string]any{"user_id": int64(1)}},
		{Payload: map[string]any{"user_id": int64(2)}},
		{Payload: map[string]any{"user_id": int64(9)}}, // no dim → dropped (inner)
	}
	out := sink.NewMemory()
	p := pipeline.New(source.NewMemory(rows), []pipeline.Stage{
		{Label: "batch", Op: mk("to-batch", map[string]any{"size": 8})},
		{Label: "join", Op: mk("vec-join", map[string]any{
			"build_key": "id", "probe_key": "user_id", "bring": "country",
			"build": []any{
				map[string]any{"id": int64(1), "country": "US"},
				map[string]any{"id": int64(2), "country": "DE"},
			},
		})},
		{Label: "rows", Op: mk("to-rows", nil)},
	}, out)
	require.NoError(t, p.Run(context.Background()))

	got := map[int64]string{}
	for _, r := range out.Records() {
		got[r.Payload["user_id"].(int64)] = r.Payload["country"].(string)
	}
	require.Equal(t, map[int64]string{1: "US", 2: "DE"}, got)
}

// TestGoldenPath_ColumnarStreamJoin wires a row source into two Schema.ID-tagged
// columnar streams (via to-batch id) that fan into a single vec-streamjoin — exercising
// the DAG fan-out/fan-in + bridges + interval join, all declaratively.
func TestGoldenPath_ColumnarStreamJoin(t *testing.T) {
	mk := func(op string, params map[string]any) core.Operator {
		o, err := buildOperator(StageSpec{Op: op, Params: params})
		require.NoError(t, err)
		return o
	}
	// Timestamps are kept close (all ≥ maxTs−window) so the match is independent of the
	// nondeterministic fan-in order — a far-future row would advance the watermark and
	// late-drop an earlier one (correct event-time semantics, but a flaky test).
	rows := []core.Record{
		{Payload: map[string]any{"side": "L", "id": int64(1), "ts": int64(100), "amt": int64(10)}},
		{Payload: map[string]any{"side": "R", "id": int64(1), "ts": int64(120), "qty": int64(5)}}, // |120-100|=20≤60 → match
		{Payload: map[string]any{"side": "L", "id": int64(2), "ts": int64(110), "amt": int64(99)}}, // no R for id=2 → no match (inner)
	}
	out := sink.NewMemory()
	p := pipeline.New(source.NewMemory(rows), []pipeline.Stage{
		{Label: "splitL", Op: mk("filter", map[string]any{"field": "side", "cmp": "eq", "value": "L"}), Next: []string{"bL"}},
		{Label: "bL", Op: mk("to-batch", map[string]any{"id": "L"}), Next: []string{"join"}},
		{Label: "splitR", Op: mk("filter", map[string]any{"field": "side", "cmp": "eq", "value": "R"}), Next: []string{"bR"}},
		{Label: "bR", Op: mk("to-batch", map[string]any{"id": "R"}), Next: []string{"join"}},
		{Label: "join", Op: mk("vec-streamjoin", map[string]any{
			"left": "L", "right": "R", "key": "id", "ts": "ts", "window": 60,
		}), Next: []string{"rows"}},
		{Label: "rows", Op: mk("to-rows", nil)},
	}, out)
	require.NoError(t, p.Run(context.Background()))

	recs := out.Records()
	require.Len(t, recs, 1) // only id=1 matches within the window
	require.Equal(t, int64(10), recs[0].Payload["amt"])
	require.Equal(t, int64(5), recs[0].Payload["qty"])
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
