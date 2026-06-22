package vector_test

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/vector"
)

// TestSlidingGroup_Overlap checks a row contributes to every overlapping window.
// size 100, hop 50 → windows overlap by 50. Key "a" at ts 10,60,120 (v 1,2,3).
func TestSlidingGroup_Overlap(t *testing.T) {
	b := tsKeyValBatch([]int64{10, 60, 120}, []string{"a", "a", "a"}, []int64{1, 2, 3})
	c := runVec(t, []*core.Batch{b},
		vector.SlidingGroup("k", "ts", 100, 50).Count("n").SumInt64("v", "s").Op())

	got := map[int64]wrow{}
	for _, r := range flatten(c) {
		got[r.win] = r
	}
	// window -50 [−50,50): ts10        → n1 s1
	// window 0   [0,100):   ts10,60    → n2 s3
	// window 50  [50,150):  ts60,120   → n2 s5
	// window 100 [100,200): ts120      → n1 s3
	want := map[int64]wrow{
		-50: {-50, "a", 1, 1},
		0:   {0, "a", 2, 3},
		50:  {50, "a", 2, 5},
		100: {100, "a", 1, 3},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d windows, want %d: %+v", len(got), len(want), got)
	}
	for w, exp := range want {
		if got[w] != exp {
			t.Fatalf("window %d: got %+v, want %+v", w, got[w], exp)
		}
	}
}

// TestSlidingGroup_MatchesTumblingWhenHopEqSize: hop == size must equal tumbling.
func TestSlidingGroup_MatchesTumblingWhenHopEqSize(t *testing.T) {
	b := tsKeyValBatch([]int64{10, 20, 110, 250}, []string{"a", "a", "b", "a"}, []int64{1, 2, 5, 7})
	slide := flatten(runVec(t, []*core.Batch{b},
		vector.SlidingGroup("k", "ts", 100, 100).Count("n").SumInt64("v", "s").Op()))
	tumble := flatten(runVec(t, []*core.Batch{b},
		vector.TumblingGroup("k", "ts", 100).Count("n").SumInt64("v", "s").Op()))
	if len(slide) != len(tumble) {
		t.Fatalf("sliding %d rows, tumbling %d", len(slide), len(tumble))
	}
	idx := func(rs []wrow) map[[2]any]wrow {
		m := map[[2]any]wrow{}
		for _, r := range rs {
			m[[2]any{r.win, r.key}] = r
		}
		return m
	}
	st, tt := idx(slide), idx(tumble)
	for k, v := range tt {
		if st[k] != v {
			t.Fatalf("key %v: sliding %+v != tumbling %+v", k, st[k], v)
		}
	}
}

// TestSessionGroup_GapAndFlush: gap 50. Key "a" ts 10,40 (one session) then 200 (a
// second). 40+50=90 ≤ wm 200 → first fires in Process; second fires at Flush.
func TestSessionGroup_GapAndFlush(t *testing.T) {
	b := tsKeyValBatch([]int64{10, 40, 200}, []string{"a", "a", "a"}, []int64{1, 2, 9})
	c := runVec(t, []*core.Batch{b},
		vector.SessionGroup("k", "ts", 50).Count("n").SumInt64("v", "s").Op())

	rows := flatten(c)
	if len(rows) != 2 {
		t.Fatalf("got %d sessions, want 2: %+v", len(rows), rows)
	}
	want := map[int64]wrow{
		10:  {10, "a", 2, 3}, // session [10,40]
		200: {200, "a", 1, 9},
	}
	for _, r := range rows {
		if r != want[r.win] {
			t.Fatalf("session %d: got %+v, want %+v", r.win, r, want[r.win])
		}
	}
	// The first emitted chunk (during Process) must hold the closed session [10],
	// proving periodic emit.
	if fw := c.Batches()[0].Int64("ts"); len(fw) != 1 || fw[0] != 10 {
		t.Fatalf("first emit should be session [10], got %v", fw)
	}
}

// TestSessionGroup_Merge: an out-of-order event bridges two sessions into one. Key "b"
// ts 10,100,50 (gap 50): 10 and 100 start apart; 50 bridges them → single [10,100].
func TestSessionGroup_Merge(t *testing.T) {
	b := tsKeyValBatch([]int64{10, 100, 50}, []string{"b", "b", "b"}, []int64{1, 2, 3})
	c := runVec(t, []*core.Batch{b},
		vector.SessionGroup("k", "ts", 50).Count("n").SumInt64("v", "s").Op())
	rows := flatten(c)
	if len(rows) != 1 {
		t.Fatalf("got %d sessions, want 1 (merged): %+v", len(rows), rows)
	}
	if rows[0] != (wrow{10, "b", 3, 6}) {
		t.Fatalf("merged session = %+v, want {10 b 3 6}", rows[0])
	}
}

// TestSessionGroup_IntKey covers the int64-key path and per-key independence.
func TestSessionGroup_IntKey(t *testing.T) {
	b := &core.Batch{
		Schema: core.Schema{Fields: []core.Field{
			{Name: "ts", Type: core.FieldTypeInt},
			{Name: "k", Type: core.FieldTypeInt},
			{Name: "v", Type: core.FieldTypeInt},
		}},
		Len: 4,
		Cols: []core.Column{
			{Kind: core.KindInt64, I64: []int64{10, 20, 10, 500}}, // 500 advances wm
			{Kind: core.KindInt64, I64: []int64{1, 1, 2, 1}},      // keys 1,1,2,1
			{Kind: core.KindInt64, I64: []int64{3, 4, 7, 1}},
		},
	}
	c := runVec(t, []*core.Batch{b},
		vector.SessionGroup("k", "ts", 50).Count("n").SumInt64("v", "s").Op())
	got := map[[2]int64]wrowI{}
	for _, bb := range c.Batches() {
		win, key, n, s := bb.Int64("ts"), bb.Int64("k"), bb.Int64("n"), bb.Int64("s")
		for i := range win {
			got[[2]int64{win[i], key[i]}] = wrowI{n[i], s[i]}
		}
	}
	// key1 session [10,20] count2 sum7 (then [500] count1 sum1); key2 [10] count1 sum7.
	if got[[2]int64{10, 1}] != (wrowI{2, 7}) || got[[2]int64{10, 2}] != (wrowI{1, 7}) || got[[2]int64{500, 1}] != (wrowI{1, 1}) {
		t.Fatalf("unexpected session aggregates: %v", got)
	}
}

type wrowI struct{ count, sum int64 }

func TestSessionGroup_Errors(t *testing.T) {
	b := tsKeyValBatch([]int64{1}, []string{"a"}, []int64{1})
	// gap <= 0
	p := pipeline.New(vector.MemSource([]*core.Batch{b}),
		[]pipeline.Stage{{Label: "s", Op: vector.SessionGroup("k", "ts", 0).Count("n").Op()}}, vector.Discard())
	if err := p.Run(context.Background()); err == nil {
		t.Fatal("expected error for gap < 1")
	}
	// bad ts column
	p2 := pipeline.New(vector.MemSource([]*core.Batch{b}),
		[]pipeline.Stage{{Label: "s", Op: vector.SessionGroup("k", "nope", 50).Count("n").Op()}}, vector.Discard())
	if err := p2.Run(context.Background()); err == nil {
		t.Fatal("expected error for missing ts column")
	}
}
