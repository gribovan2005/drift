package vector_test

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/vector"
)

// makeBatch builds a chunk with a string key column, an int64 value column "qty",
// and a float64 value column "amt".
func makeBatch(keys []string, qty []int64, amt []float64) *core.Batch {
	return &core.Batch{
		Schema: core.Schema{Fields: []core.Field{
			{Name: "k", Type: core.FieldTypeString},
			{Name: "qty", Type: core.FieldTypeInt},
			{Name: "amt", Type: core.FieldTypeFloat},
		}},
		Len: len(keys),
		Cols: []core.Column{
			{Kind: core.KindString, Str: keys},
			{Kind: core.KindInt64, I64: qty},
			{Kind: core.KindFloat64, F64: amt},
		},
	}
}

// runGroup runs group g over the given batches in one pipeline and returns the single
// result chunk (nil if none).
func runGroup(t *testing.T, batches []*core.Batch, op core.Operator) *core.Batch {
	t.Helper()
	out := vector.Collect()
	p := pipeline.New(vector.MemSource(batches), []pipeline.Stage{{Label: "g", Op: op}}, out)
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	bs := out.Batches()
	if len(bs) == 0 {
		return nil
	}
	if len(bs) != 1 {
		t.Fatalf("expected 1 result chunk, got %d", len(bs))
	}
	return bs[0]
}

// resultMap turns a group-by result chunk into key -> (count, sumQty, maxQty, sumAmt).
type row struct {
	count, sumQty, maxQty int64
	sumAmt                float64
}

func resultMap(t *testing.T, b *core.Batch) map[string]row {
	t.Helper()
	m := map[string]row{}
	if b == nil {
		return m
	}
	keys := b.String("k")
	n := b.Int64("n")
	sq := b.Int64("sumQty")
	mq := b.Int64("maxQty")
	sa := b.Float64("sumAmt")
	if keys == nil || n == nil || sq == nil || mq == nil || sa == nil {
		t.Fatalf("result chunk missing expected columns: %+v", b.Schema)
	}
	for i := 0; i < b.Len; i++ {
		m[keys[i]] = row{count: n[i], sumQty: sq[i], maxQty: mq[i], sumAmt: sa[i]}
	}
	return m
}

func gb() *vector.Group {
	return vector.GroupBy("k").Count("n").SumInt64("qty", "sumQty").MaxInt64("qty", "maxQty").SumFloat64("amt", "sumAmt")
}

// TestMergeGroups_MatchesGlobal proves a distributed group-by (N independent lanes,
// each over an arbitrary, NOT key-sharded slice of the input → partials → MergeOp)
// equals a single global group-by over the whole input.
func TestMergeGroups_MatchesGlobal(t *testing.T) {
	// Synthetic data: 4 keys, values spread so each key lands in multiple lanes.
	all := []*core.Batch{
		makeBatch([]string{"a", "b", "a", "c"}, []int64{1, 2, 3, 4}, []float64{1.5, 2.0, 0.5, 4.0}),
		makeBatch([]string{"b", "c", "d", "a"}, []int64{5, 6, 7, 8}, []float64{1.0, 1.0, 9.0, 2.0}),
		makeBatch([]string{"d", "a", "c", "b"}, []int64{9, 2, 1, 3}, []float64{0.1, 0.2, 0.3, 0.4}),
		makeBatch([]string{"c", "d", "a", "b"}, []int64{10, 11, 12, 13}, []float64{5.0, 6.0, 7.0, 8.0}),
	}

	// Ground truth: one global group-by over everything.
	want := resultMap(t, runGroup(t, all, gb().Op()))

	// Distributed: 3 lanes, batches split round-robin (deliberately not key-sharded —
	// keys are scattered across lanes, so correctness depends on the merge).
	const lanesN = 3
	laneBatches := make([][]*core.Batch, lanesN)
	for i, b := range all {
		l := i % lanesN
		laneBatches[l] = append(laneBatches[l], b)
	}
	// One key is forced into a single batch too — fine, partials still combine.

	collectors := make([]*vector.Collector, lanesN)
	lanes := make([]*pipeline.Pipeline, lanesN)
	for l := range lanes {
		collectors[l] = vector.Collect()
		lanes[l] = pipeline.New(
			vector.MemSource(laneBatches[l]),
			[]pipeline.Stage{{Label: "partial", Op: gb().Op()}},
			collectors[l],
		)
	}
	if err := pipeline.RunLanes(context.Background(), lanes...); err != nil {
		t.Fatalf("run lanes: %v", err)
	}

	// Gather every lane's partial result chunk and merge them.
	var partials []*core.Batch
	for _, c := range collectors {
		partials = append(partials, c.Batches()...)
	}
	got := resultMap(t, runGroup(t, partials, gb().MergeOp()))

	if len(got) != len(want) {
		t.Fatalf("merged has %d keys, want %d", len(got), len(want))
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Fatalf("key %q missing from merged result", k)
		}
		if g != w {
			t.Fatalf("key %q: merged %+v != global %+v", k, g, w)
		}
	}
}

// TestMergeGroups_IntKey covers the int64-key path and a single empty lane.
func TestMergeGroups_IntKey(t *testing.T) {
	mk := func(keys []int64, qty []int64) *core.Batch {
		return &core.Batch{
			Schema: core.Schema{Fields: []core.Field{
				{Name: "k", Type: core.FieldTypeInt},
				{Name: "qty", Type: core.FieldTypeInt},
			}},
			Len:  len(keys),
			Cols: []core.Column{{Kind: core.KindInt64, I64: keys}, {Kind: core.KindInt64, I64: qty}},
		}
	}
	g := vector.GroupBy("k").Count("n").SumInt64("qty", "s")

	all := []*core.Batch{
		mk([]int64{1, 2, 1}, []int64{10, 20, 30}),
		mk([]int64{2, 3, 1}, []int64{1, 2, 3}),
	}
	want := runGroup(t, all, g.Op())

	// lane 0 = batch 0, lane 1 = batch 1, lane 2 = empty (no input).
	c0, c1, c2 := vector.Collect(), vector.Collect(), vector.Collect()
	lanes := []*pipeline.Pipeline{
		pipeline.New(vector.MemSource(all[:1]), []pipeline.Stage{{Label: "p", Op: g.Op()}}, c0),
		pipeline.New(vector.MemSource(all[1:]), []pipeline.Stage{{Label: "p", Op: g.Op()}}, c1),
		pipeline.New(vector.MemSource(nil), []pipeline.Stage{{Label: "p", Op: g.Op()}}, c2),
	}
	if err := pipeline.RunLanes(context.Background(), lanes...); err != nil {
		t.Fatalf("run lanes: %v", err)
	}
	var partials []*core.Batch
	partials = append(partials, c0.Batches()...)
	partials = append(partials, c1.Batches()...)
	partials = append(partials, c2.Batches()...)
	got := runGroup(t, partials, g.MergeOp())

	wk, wn, ws := want.Int64("k"), want.Int64("n"), want.Int64("s")
	gk, gn, gs := got.Int64("k"), got.Int64("n"), got.Int64("s")
	if got.Len != want.Len {
		t.Fatalf("merged %d keys, want %d", got.Len, want.Len)
	}
	for i := 0; i < want.Len; i++ {
		if gk[i] != wk[i] || gn[i] != wn[i] || gs[i] != ws[i] {
			t.Fatalf("row %d: merged (k=%d,n=%d,s=%d) != global (k=%d,n=%d,s=%d)",
				i, gk[i], gn[i], gs[i], wk[i], wn[i], ws[i])
		}
	}
}

// TestMergeGroups_BadColumn surfaces a partial chunk missing an expected column.
func TestMergeGroups_BadColumn(t *testing.T) {
	bad := &core.Batch{
		Schema: core.Schema{Fields: []core.Field{{Name: "k", Type: core.FieldTypeString}}},
		Len:    1,
		Cols:   []core.Column{{Kind: core.KindString, Str: []string{"a"}}},
	}
	out := vector.Collect()
	p := pipeline.New(vector.MemSource([]*core.Batch{bad}),
		[]pipeline.Stage{{Label: "m", Op: gb().MergeOp()}}, out)
	if err := p.Run(context.Background()); err == nil {
		t.Fatal("expected error merging a chunk missing aggregate columns")
	}
}
