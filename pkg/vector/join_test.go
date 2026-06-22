package vector_test

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/vector"
	"github.com/gribovan2005/drift/sdk"
)

// dimInt builds a dimension batch: int64 key "id" + string "country" + int64 "tier".
func dimInt(ids []int64, countries []string, tiers []int64) *core.Batch {
	return &core.Batch{
		Schema: core.Schema{Fields: []core.Field{
			{Name: "id", Type: core.FieldTypeInt},
			{Name: "country", Type: core.FieldTypeString},
			{Name: "tier", Type: core.FieldTypeInt},
		}},
		Len: len(ids),
		Cols: []core.Column{
			{Kind: core.KindInt64, I64: ids},
			{Kind: core.KindString, Str: countries},
			{Kind: core.KindInt64, I64: tiers},
		},
	}
}

// probeInt builds a probe batch: int64 "user_id" + int64 "amt".
func probeInt(uids, amts []int64) *core.Batch {
	return &core.Batch{
		Schema: core.Schema{Fields: []core.Field{
			{Name: "user_id", Type: core.FieldTypeInt},
			{Name: "amt", Type: core.FieldTypeInt},
		}},
		Len: len(uids),
		Cols: []core.Column{
			{Kind: core.KindInt64, I64: uids},
			{Kind: core.KindInt64, I64: amts},
		},
	}
}

func TestHashJoin_Int64Enrich(t *testing.T) {
	dim := dimInt([]int64{1, 2, 3}, []string{"US", "DE", "JP"}, []int64{10, 20, 30})
	// probe: user 2 matches, 99 doesn't, 1 matches.
	probe := probeInt([]int64{2, 99, 1}, []int64{100, 200, 300})
	c := runVec(t, []*core.Batch{probe},
		vector.HashJoin([]*core.Batch{dim}, "id", "user_id").
			Bring("country", "country").Bring("tier", "tier").Op())

	res := c.Batches()[0]
	if res.Len != 2 {
		t.Fatalf("matched rows = %d, want 2 (99 dropped)", res.Len)
	}
	uid := res.Int64("user_id")
	amt := res.Int64("amt")
	country := res.String("country")
	tier := res.Int64("tier")
	// row order preserved among matches: user 2 then user 1.
	if uid[0] != 2 || amt[0] != 100 || country[0] != "DE" || tier[0] != 20 {
		t.Fatalf("row0 = uid%d amt%d %s %d", uid[0], amt[0], country[0], tier[0])
	}
	if uid[1] != 1 || amt[1] != 300 || country[1] != "US" || tier[1] != 10 {
		t.Fatalf("row1 = uid%d amt%d %s %d", uid[1], amt[1], country[1], tier[1])
	}
}

func TestHashJoin_StringKey_MultiChunk(t *testing.T) {
	dim := &core.Batch{
		Schema: core.Schema{Fields: []core.Field{
			{Name: "code", Type: core.FieldTypeString},
			{Name: "rate", Type: core.FieldTypeFloat},
		}},
		Len: 2,
		Cols: []core.Column{
			{Kind: core.KindString, Str: []string{"EUR", "USD"}},
			{Kind: core.KindFloat64, F64: []float64{1.1, 1.0}},
		},
	}
	mk := func(codes []string) *core.Batch {
		return &core.Batch{
			Schema: core.Schema{Fields: []core.Field{{Name: "cur", Type: core.FieldTypeString}}},
			Len:    len(codes),
			Cols:   []core.Column{{Kind: core.KindString, Str: codes}},
		}
	}
	c := runVec(t, []*core.Batch{mk([]string{"USD", "GBP"}), mk([]string{"EUR"})},
		vector.HashJoin([]*core.Batch{dim}, "code", "cur").Bring("rate", "rate").Op())

	var total int
	for _, b := range c.Batches() {
		total += b.Len
	}
	if total != 2 { // USD and EUR match; GBP dropped
		t.Fatalf("matched = %d, want 2", total)
	}
}

func TestHashJoin_NoMatch(t *testing.T) {
	dim := dimInt([]int64{1}, []string{"US"}, []int64{1})
	probe := probeInt([]int64{7, 8}, []int64{1, 2})
	c := runVec(t, []*core.Batch{probe},
		vector.HashJoin([]*core.Batch{dim}, "id", "user_id").Bring("country", "country").Op())
	if res := c.Batches()[0]; res.Len != 0 {
		t.Fatalf("expected 0 matched rows, got %d", res.Len)
	}
}

func TestHashJoin_Errors(t *testing.T) {
	dim := dimInt([]int64{1}, []string{"US"}, []int64{1})
	run := func(j core.Operator, probe *core.Batch) error {
		p := pipeline.New(vector.MemSource([]*core.Batch{probe}),
			[]pipeline.Stage{{Label: "j", Op: j}}, vector.Discard())
		return p.Run(context.Background())
	}
	// missing brought field
	if err := run(vector.HashJoin([]*core.Batch{dim}, "id", "user_id").Bring("nope", "x").Op(), probeInt([]int64{1}, []int64{1})); err == nil {
		t.Fatal("expected error for missing brought field")
	}
	// probe key kind mismatch (build int64, probe string)
	strProbe := &core.Batch{Schema: core.Schema{Fields: []core.Field{{Name: "user_id", Type: core.FieldTypeString}}},
		Len: 1, Cols: []core.Column{{Kind: core.KindString, Str: []string{"1"}}}}
	if err := run(vector.HashJoin([]*core.Batch{dim}, "id", "user_id").Bring("country", "c").Op(), strProbe); err == nil {
		t.Fatal("expected error for probe key kind mismatch")
	}
	// empty build
	if err := run(vector.HashJoin(nil, "id", "user_id").Op(), probeInt([]int64{1}, []int64{1})); err == nil {
		t.Fatal("expected error for empty build")
	}
}

func TestHashJoin_SDK_AndParallelSafe(t *testing.T) {
	dim := dimInt([]int64{1, 2, 3, 4}, []string{"A", "B", "C", "D"}, []int64{1, 2, 3, 4})
	// many probe chunks across keys 1..4
	var batches []*core.Batch
	for i := 0; i < 8; i++ {
		batches = append(batches, probeInt([]int64{int64(i%4 + 1)}, []int64{int64(i)}))
	}
	c := vector.Collect()
	err := sdk.New().
		From(vector.MemSource(batches)).
		Apply(vector.Parallel(4, func() core.Operator {
			return vector.HashJoin([]*core.Batch{dim}, "id", "user_id").Bring("country", "country").Op()
		})).
		To(c).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	var total int
	for _, b := range c.Batches() {
		// every row must have a country column populated
		if cs := b.String("country"); len(cs) != b.Len {
			t.Fatalf("country column len %d != batch len %d", len(cs), b.Len)
		}
		total += b.Len
	}
	if total != 8 { // all 8 probe rows match (keys 1..4 all in dim)
		t.Fatalf("matched = %d, want 8", total)
	}
}
