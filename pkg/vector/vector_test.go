package vector_test

import (
	"context"
	"sort"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/vector"
	"github.com/gribovan2005/drift/sdk"
)

// runVec runs ops over the given batches through the REAL pipeline executor and
// returns the collected surviving batches.
func runVec(t *testing.T, batches []*core.Batch, ops ...core.Operator) *vector.Collector {
	t.Helper()
	stages := make([]pipeline.Stage, len(ops))
	for i, op := range ops {
		stages[i] = pipeline.Stage{Label: "vec", Op: op}
	}
	c := vector.Collect()
	p := pipeline.New(vector.MemSource(batches), stages, c)
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	return c
}

func TestMapInt64_ThroughPipeline(t *testing.T) {
	batches := vector.GenInt64("v", 3, 100, func(i int) int64 { return int64(i) }) // 0..299
	c := runVec(t, batches, vector.MapInt64("v", func(x int64) int64 { return x + 1 }))
	if c.Rows() != 300 {
		t.Fatalf("rows = %d, want 300", c.Rows())
	}
	// Every value incremented.
	for _, b := range c.Batches() {
		col := b.Int64("v")
		for i := range col {
			if col[i] == 0 {
				t.Fatal("found unincremented value")
			}
		}
	}
	// Spot-check first batch first value: 0 -> 1.
	if got := c.Batches()[0].Int64("v")[0]; got != 1 {
		t.Fatalf("first value = %d, want 1", got)
	}
}

func TestFilterInt64_CompactionKeepsColumnsAligned(t *testing.T) {
	// Two columns: v = 0..9, paired = v*10. Keep even v; paired must stay aligned.
	b := &core.Batch{
		Schema: core.Schema{Fields: []core.Field{
			{Name: "v", Type: core.FieldTypeInt},
			{Name: "paired", Type: core.FieldTypeInt},
		}},
		Len: 10,
		Cols: []core.Column{
			{Kind: core.KindInt64, I64: []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
			{Kind: core.KindInt64, I64: []int64{0, 10, 20, 30, 40, 50, 60, 70, 80, 90}},
		},
	}
	c := runVec(t, []*core.Batch{b}, vector.FilterInt64("v", func(x int64) bool { return x%2 == 0 }))
	if c.Rows() != 5 {
		t.Fatalf("rows = %d, want 5", c.Rows())
	}
	out := c.Batches()[0]
	v := out.Int64("v")
	paired := out.Int64("paired")
	for i := range v {
		if v[i]%2 != 0 {
			t.Fatalf("kept odd value %d", v[i])
		}
		if paired[i] != v[i]*10 {
			t.Fatalf("columns misaligned: v=%d paired=%d", v[i], paired[i])
		}
	}
}

func TestEndToEnd_SDK_MatchesRowSemantics(t *testing.T) {
	// Filter(even) then Map(+1) over 0..999 via the vectorized SDK path.
	batches := vector.GenInt64("v", 10, 100, func(i int) int64 { return int64(i) })
	c := vector.Collect()
	err := sdk.New().
		From(vector.MemSource(batches)).
		Apply(vector.FilterInt64("v", func(x int64) bool { return x%2 == 0 })).
		Apply(vector.MapInt64("v", func(x int64) int64 { return x + 1 })).
		To(c).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	// Expected: evens 0,2,..,998 → +1 = 1,3,..,999 (500 rows).
	var got []int64
	for _, b := range c.Batches() {
		got = append(got, b.Int64("v")...)
	}
	if len(got) != 500 {
		t.Fatalf("got %d rows, want 500", len(got))
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if got[0] != 1 || got[len(got)-1] != 999 {
		t.Fatalf("range = [%d..%d], want [1..999]", got[0], got[len(got)-1])
	}
}

func TestToRows_Expands(t *testing.T) {
	batches := vector.GenInt64("v", 1, 3, func(i int) int64 { return int64(i * 5) }) // 0,5,10
	c := runVecRows(t, batches)
	if len(c) != 3 {
		t.Fatalf("got %d rows, want 3", len(c))
	}
	if c[0].Payload["v"].(int64) != 0 || c[2].Payload["v"].(int64) != 10 {
		t.Fatalf("unexpected expanded payloads: %+v", c)
	}
}

// runVecRows runs ToRows and collects the expanded row records.
func runVecRows(t *testing.T, batches []*core.Batch) []core.Record {
	t.Helper()
	rows := &rowSink{}
	p := pipeline.New(vector.MemSource(batches), []pipeline.Stage{{Label: "torows", Op: vector.ToRows()}}, rows)
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	return rows.records
}

type rowSink struct{ records []core.Record }

func (s *rowSink) Write(ctx context.Context, ch <-chan core.Record) error {
	for r := range ch {
		s.records = append(s.records, r)
	}
	return nil
}
