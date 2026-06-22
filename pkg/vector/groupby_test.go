package vector_test

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/vector"
	"github.com/gribovan2005/drift/sdk"
)

func TestGroupBy_SDK_EndToEnd(t *testing.T) {
	b := strAmtBatch([]string{"x", "y", "x"}, []float64{1, 2, 3}, []int64{1, 1, 1})
	c := vector.Collect()
	err := sdk.New().
		From(vector.MemSource([]*core.Batch{b})).
		Apply(vector.GroupBy("cat").Count("n").SumFloat64("amt", "total").Op()).
		To(c).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	res := c.Batches()[0]
	keys, n, total := res.String("cat"), res.Int64("n"), res.Float64("total")
	if len(keys) != 2 {
		t.Fatalf("groups = %d, want 2", len(keys))
	}
	// x: n2 total4 ; y: n1 total2
	got := map[string][2]float64{}
	for i, k := range keys {
		got[k] = [2]float64{float64(n[i]), total[i]}
	}
	if got["x"] != [2]float64{2, 4} || got["y"] != [2]float64{1, 2} {
		t.Fatalf("unexpected: %v", got)
	}
}

// strAmtBatch builds a chunk with a string key "cat" + float "amt" + int "q".
func strAmtBatch(cats []string, amts []float64, qs []int64) *core.Batch {
	return &core.Batch{
		Schema: core.Schema{Fields: []core.Field{
			{Name: "cat", Type: core.FieldTypeString},
			{Name: "amt", Type: core.FieldTypeFloat},
			{Name: "q", Type: core.FieldTypeInt},
		}},
		Len: len(cats),
		Cols: []core.Column{
			{Kind: core.KindString, Str: cats},
			{Kind: core.KindFloat64, F64: amts},
			{Kind: core.KindInt64, I64: qs},
		},
	}
}

// agg looks up a result row by key value and returns its agg column value.
func TestGroupBy_StringKey_CountSum_MultiChunk(t *testing.T) {
	b1 := strAmtBatch([]string{"a", "b", "a"}, []float64{1, 2, 3}, []int64{1, 1, 1})
	b2 := strAmtBatch([]string{"b", "c", "a"}, []float64{4, 5, 6}, []int64{1, 1, 1})
	c := runVec(t, []*core.Batch{b1, b2},
		vector.GroupBy("cat").Count("n").SumFloat64("amt", "total").Op())

	if len(c.Batches()) != 1 {
		t.Fatalf("expected 1 result chunk, got %d", len(c.Batches()))
	}
	res := c.Batches()[0]
	keys := res.String("cat")
	n := res.Int64("n")
	total := res.Float64("total")
	if len(keys) != 3 { // a,b,c sorted
		t.Fatalf("groups = %d, want 3", len(keys))
	}
	want := map[string]struct {
		n     int64
		total float64
	}{"a": {3, 10}, "b": {2, 6}, "c": {1, 5}}
	for i, k := range keys {
		w := want[k]
		if n[i] != w.n || total[i] != w.total {
			t.Fatalf("key %q: n=%d total=%v, want n=%d total=%v", k, n[i], total[i], w.n, w.total)
		}
	}
	// sorted order
	if keys[0] != "a" || keys[1] != "b" || keys[2] != "c" {
		t.Fatalf("keys not sorted: %v", keys)
	}
}

func TestGroupBy_Int64Key_SumMax(t *testing.T) {
	b := &core.Batch{
		Schema: core.Schema{Fields: []core.Field{
			{Name: "k", Type: core.FieldTypeInt},
			{Name: "v", Type: core.FieldTypeInt},
		}},
		Len: 5,
		Cols: []core.Column{
			{Kind: core.KindInt64, I64: []int64{10, 20, 10, 20, 10}},
			{Kind: core.KindInt64, I64: []int64{1, 7, 3, 2, 9}},
		},
	}
	c := runVec(t, []*core.Batch{b},
		vector.GroupBy("k").SumInt64("v", "sum").MaxInt64("v", "max").Op())
	res := c.Batches()[0]
	keys := res.Int64("k")
	sum := res.Int64("sum")
	mx := res.Int64("max")
	// k=10: v {1,3,9} sum13 max9 ; k=20: v {7,2} sum9 max7
	want := map[int64][2]int64{10: {13, 9}, 20: {9, 7}}
	for i, k := range keys {
		if sum[i] != want[k][0] || mx[i] != want[k][1] {
			t.Fatalf("key %d: sum=%d max=%d, want %v", k, sum[i], mx[i], want[k])
		}
	}
}

func TestGroupBy_Empty(t *testing.T) {
	c := runVec(t, nil, vector.GroupBy("cat").Count("n").Op())
	if len(c.Batches()) != 1 {
		t.Fatalf("expected 1 (empty) result chunk, got %d", len(c.Batches()))
	}
	if c.Batches()[0].Len != 0 {
		t.Fatalf("expected zero rows, got %d", c.Batches()[0].Len)
	}
}

func TestGroupBy_BadKey_Errors(t *testing.T) {
	b := strAmtBatch([]string{"a"}, []float64{1}, []int64{1})
	p := pipeline.New(
		vector.MemSource([]*core.Batch{b}),
		[]pipeline.Stage{{Label: "g", Op: vector.GroupBy("nope").Count("n").Op()}},
		vector.Discard(),
	)
	if err := p.Run(context.Background()); err == nil {
		t.Fatal("expected error for missing key column")
	}
}
