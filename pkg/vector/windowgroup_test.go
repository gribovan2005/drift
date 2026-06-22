package vector_test

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/vector"
	"github.com/gribovan2005/drift/sdk"
)

// tsKeyValBatch builds a chunk with int64 ts "ts", string key "k", int64 value "v".
func tsKeyValBatch(ts []int64, ks []string, vs []int64) *core.Batch {
	return &core.Batch{
		Schema: core.Schema{Fields: []core.Field{
			{Name: "ts", Type: core.FieldTypeInt},
			{Name: "k", Type: core.FieldTypeString},
			{Name: "v", Type: core.FieldTypeInt},
		}},
		Len: len(ts),
		Cols: []core.Column{
			{Kind: core.KindInt64, I64: ts},
			{Kind: core.KindString, Str: ks},
			{Kind: core.KindInt64, I64: vs},
		},
	}
}

type wrow struct {
	win        int64
	key        string
	count, sum int64
}

func flatten(c *vector.Collector) []wrow {
	var out []wrow
	for _, b := range c.Batches() {
		win, key, n, s := b.Int64("ts"), b.String("k"), b.Int64("n"), b.Int64("s")
		for i := range win {
			out = append(out, wrow{win[i], key[i], n[i], s[i]})
		}
	}
	return out
}

func TestTumblingGroup_WatermarkFiringAndFlush(t *testing.T) {
	// size 100, lateness 0. Rows in windows 0,100,200; ts=250 advances the
	// watermark to 250 → windows 0 and 100 fire during Process; window 200 fires
	// at Flush.
	b := tsKeyValBatch(
		[]int64{10, 20, 110, 250},
		[]string{"a", "a", "b", "a"},
		[]int64{1, 2, 5, 7},
	)
	c := runVec(t, []*core.Batch{b},
		vector.TumblingGroup("k", "ts", 100).Count("n").SumInt64("v", "s").Op())

	rows := flatten(c)
	if len(rows) != 3 {
		t.Fatalf("got %d result rows, want 3: %+v", len(rows), rows)
	}
	want := map[int64]wrow{
		0:   {0, "a", 2, 3},
		100: {100, "b", 1, 5},
		200: {200, "a", 1, 7},
	}
	for _, r := range rows {
		w := want[r.win]
		if r != w {
			t.Fatalf("window %d: got %+v, want %+v", r.win, r, w)
		}
	}
	// The first emitted chunk (during Process) must hold the closed windows 0 & 100,
	// proving periodic emit (not all-at-flush).
	first := c.Batches()[0]
	fw := first.Int64("ts")
	if len(fw) != 2 || fw[0] != 0 || fw[1] != 100 {
		t.Fatalf("first emit should be windows [0 100], got %v", fw)
	}
}

func TestTumblingGroup_LateDropped(t *testing.T) {
	// chunk1 advances watermark to 300 → window 0 fires (only ts=10 row).
	b1 := tsKeyValBatch([]int64{10, 300}, []string{"a", "a"}, []int64{1, 1})
	// chunk2: ts=50 belongs to the already-fired window 0 → dropped as late.
	b2 := tsKeyValBatch([]int64{50}, []string{"a"}, []int64{9})
	c := runVec(t, []*core.Batch{b1, b2},
		vector.TumblingGroup("k", "ts", 100).Count("n").SumInt64("v", "s").Op())

	rows := flatten(c)
	var win0 *wrow
	for i := range rows {
		if rows[i].win == 0 {
			win0 = &rows[i]
		}
	}
	if win0 == nil {
		t.Fatal("window 0 missing")
	}
	if win0.count != 1 || win0.sum != 1 {
		t.Fatalf("window 0 = %+v, want count 1 sum 1 (late ts=50 dropped)", *win0)
	}
}

func TestTumblingGroup_Int64Key_Max(t *testing.T) {
	b := &core.Batch{
		Schema: core.Schema{Fields: []core.Field{
			{Name: "ts", Type: core.FieldTypeInt},
			{Name: "k", Type: core.FieldTypeInt},
			{Name: "v", Type: core.FieldTypeInt},
		}},
		Len: 4,
		Cols: []core.Column{
			{Kind: core.KindInt64, I64: []int64{5, 7, 9, 500}}, // last advances wm past window 0
			{Kind: core.KindInt64, I64: []int64{1, 1, 2, 1}},
			{Kind: core.KindInt64, I64: []int64{3, 8, 4, 1}},
		},
	}
	c := runVec(t, []*core.Batch{b},
		vector.TumblingGroup("k", "ts", 100).MaxInt64("v", "mx").Op())
	// window 0: key1 max(3,8)=8 ; key2 max(4)=4 ; window 500: key1 max(1)=1
	got := map[[2]int64]int64{}
	for _, b := range c.Batches() {
		win, key, mx := b.Int64("ts"), b.Int64("k"), b.Int64("mx")
		for i := range win {
			got[[2]int64{win[i], key[i]}] = mx[i]
		}
	}
	if got[[2]int64{0, 1}] != 8 || got[[2]int64{0, 2}] != 4 || got[[2]int64{500, 1}] != 1 {
		t.Fatalf("unexpected maxes: %v", got)
	}
}

func TestTumblingGroup_SDK_EndToEnd(t *testing.T) {
	b := tsKeyValBatch([]int64{10, 20, 1000}, []string{"x", "x", "x"}, []int64{1, 2, 9})
	c := vector.Collect()
	err := sdk.New().
		From(vector.MemSource([]*core.Batch{b})).
		Apply(vector.TumblingGroup("k", "ts", 100).Lateness(0).Count("n").SumInt64("v", "s").Op()).
		To(c).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	rows := flatten(c)
	// window 0: x count2 sum3 ; window 1000: x count1 sum9
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2: %+v", len(rows), rows)
	}
}

func TestTumblingGroup_BadTs(t *testing.T) {
	// key present but no int64 ts column named "ts".
	b := strAmtBatch([]string{"a"}, []float64{1}, []int64{1}) // has cat/amt/q, no "ts"
	p := pipeline.New(
		vector.MemSource([]*core.Batch{b}),
		[]pipeline.Stage{{Label: "w", Op: vector.TumblingGroup("cat", "ts", 100).Count("n").Op()}},
		vector.Discard(),
	)
	if err := p.Run(context.Background()); err == nil {
		t.Fatal("expected error for missing ts column")
	}
}
