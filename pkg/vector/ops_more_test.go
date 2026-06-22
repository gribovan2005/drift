package vector_test

import (
	"context"
	"strings"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/vector"
)

func strBoolBatch() *core.Batch {
	return &core.Batch{
		Schema: core.Schema{Fields: []core.Field{
			{Name: "cat", Type: core.FieldTypeString},
			{Name: "ok", Type: core.FieldTypeBool},
			{Name: "v", Type: core.FieldTypeInt},
		}},
		Len: 4,
		Cols: []core.Column{
			{Kind: core.KindString, Str: []string{"a", "b", "a", "c"}},
			{Kind: core.KindBool, B: []bool{true, false, true, false}},
			{Kind: core.KindInt64, I64: []int64{1, 2, 3, 4}},
		},
	}
}

func TestMapString(t *testing.T) {
	c := runVec(t, []*core.Batch{strBoolBatch()}, vector.MapString("cat", strings.ToUpper))
	got := c.Batches()[0].String("cat")
	if got[0] != "A" || got[3] != "C" {
		t.Fatalf("MapString = %v", got)
	}
}

func TestFilterString_KeepsAligned(t *testing.T) {
	// keep cat=="a"; v must stay aligned (rows 0 and 2 → v 1 and 3).
	c := runVec(t, []*core.Batch{strBoolBatch()}, vector.FilterString("cat", func(s string) bool { return s == "a" }))
	b := c.Batches()[0]
	if b.Len != 2 {
		t.Fatalf("len = %d, want 2", b.Len)
	}
	if v := b.Int64("v"); v[0] != 1 || v[1] != 3 {
		t.Fatalf("aligned v = %v, want [1 3]", v)
	}
}

func TestFilterBool(t *testing.T) {
	c := runVec(t, []*core.Batch{strBoolBatch()}, vector.FilterBool("ok", func(b bool) bool { return b }))
	b := c.Batches()[0]
	if b.Len != 2 {
		t.Fatalf("len = %d, want 2 (ok==true rows)", b.Len)
	}
	if v := b.Int64("v"); v[0] != 1 || v[1] != 3 {
		t.Fatalf("aligned v = %v, want [1 3]", v)
	}
}

// rowResult runs ops whose output is a row Record (aggregates) and returns them.
func rowResult(t *testing.T, batches []*core.Batch, op core.Operator) []core.Record {
	t.Helper()
	s := &aggSink{}
	p := pipeline.New(vector.MemSource(batches), []pipeline.Stage{{Label: "agg", Op: op}}, s)
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	return s.records
}

type aggSink struct{ records []core.Record }

func (s *aggSink) Write(ctx context.Context, ch <-chan core.Record) error {
	for r := range ch {
		s.records = append(s.records, r)
	}
	return nil
}

func TestAggregates(t *testing.T) {
	batches := vector.GenInt64("v", 5, 100, func(i int) int64 { return int64(i) }) // 0..499

	sum := rowResult(t, batches, vector.SumInt64("v", "total"))
	if len(sum) != 1 || sum[0].Payload["total"].(int64) != 124750 { // sum 0..499
		t.Fatalf("SumInt64 = %+v", sum)
	}
	cnt := rowResult(t, batches, vector.CountRows("n"))
	if cnt[0].Payload["n"].(int64) != 500 {
		t.Fatalf("CountRows = %+v", cnt)
	}
	mx := rowResult(t, batches, vector.MaxInt64("v", "max"))
	if mx[0].Payload["max"].(int64) != 499 {
		t.Fatalf("MaxInt64 = %+v", mx)
	}
}

func TestParallelVectorStage(t *testing.T) {
	// 20 chunks of 100 → MapInt64(+1) across 4 parallel shards; result still all
	// incremented and complete.
	batches := vector.GenInt64("v", 20, 100, func(i int) int64 { return int64(i) })
	c := runVec(t, batches, vector.Parallel(4, func() core.Operator {
		return vector.MapInt64("v", func(x int64) int64 { return x + 1 })
	}))
	if c.Rows() != 2000 {
		t.Fatalf("rows = %d, want 2000", c.Rows())
	}
	// sum of original 0..1999 is 1999000; after +1 each it's +2000.
	var sum int64
	for _, b := range c.Batches() {
		for _, v := range b.Int64("v") {
			sum += v
		}
	}
	if want := int64(1999000 + 2000); sum != want {
		t.Fatalf("sum = %d, want %d", sum, want)
	}
}
