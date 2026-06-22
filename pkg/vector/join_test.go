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

func TestHashJoin_LeftOuter(t *testing.T) {
	dim := dimInt([]int64{1, 2, 3}, []string{"US", "DE", "JP"}, []int64{10, 20, 30})
	// probe: user 2 matches, 99 doesn't (→ NULL brought cells), 1 matches.
	probe := probeInt([]int64{2, 99, 1}, []int64{100, 200, 300})
	c := runVec(t, []*core.Batch{probe},
		vector.HashJoin([]*core.Batch{dim}, "id", "user_id").
			Bring("country", "country").Bring("tier", "tier").LeftOuter().Op())

	res := c.Batches()[0]
	if res.Len != 3 {
		t.Fatalf("left-outer rows = %d, want 3 (99 kept with NULLs)", res.Len)
	}
	uid := res.Int64("user_id")
	amt := res.Int64("amt")
	country := res.String("country")
	tier := res.Int64("tier")
	cNull := res.IsNull("country")
	tNull := res.IsNull("tier")
	if cNull == nil || tNull == nil {
		t.Fatal("expected null masks on brought columns")
	}
	// row order preserved: 2 (match), 99 (no match → NULL), 1 (match).
	if uid[0] != 2 || amt[0] != 100 || country[0] != "DE" || tier[0] != 20 || cNull[0] || tNull[0] {
		t.Fatalf("row0 = uid%d amt%d %q tier%d cNull%v", uid[0], amt[0], country[0], tier[0], cNull[0])
	}
	if uid[1] != 99 || amt[1] != 200 || !cNull[1] || !tNull[1] {
		t.Fatalf("row1 (unmatched) should be NULL brought: uid%d amt%d cNull%v tNull%v", uid[1], amt[1], cNull[1], tNull[1])
	}
	if uid[2] != 1 || amt[2] != 300 || country[2] != "US" || tier[2] != 10 || cNull[2] || tNull[2] {
		t.Fatalf("row2 = uid%d amt%d %q tier%d cNull%v", uid[2], amt[2], country[2], tier[2], cNull[2])
	}
}

// TestHashJoin_LeftOuter_AllMatch keeps the null mask nil (zero-overhead) when every
// probe row matches — left-outer must not differ from inner in that case.
func TestHashJoin_LeftOuter_AllMatch(t *testing.T) {
	dim := dimInt([]int64{1, 2}, []string{"US", "DE"}, []int64{10, 20})
	probe := probeInt([]int64{1, 2}, []int64{5, 6})
	c := runVec(t, []*core.Batch{probe},
		vector.HashJoin([]*core.Batch{dim}, "id", "user_id").Bring("country", "country").LeftOuter().Op())
	res := c.Batches()[0]
	if res.Len != 2 {
		t.Fatalf("rows = %d, want 2", res.Len)
	}
	if res.IsNull("country") != nil {
		t.Fatal("all matched → null mask should stay nil (zero-overhead)")
	}
}

// TestHashJoin_LeftOuter_FilterDownstream exercises CopyRow/Truncate carrying the null
// mask: a Filter after a left-outer join compacts a batch whose brought column has
// NULLs, so the surviving rows must keep the right null bits.
func TestHashJoin_LeftOuter_FilterDownstream(t *testing.T) {
	dim := dimInt([]int64{1, 3}, []string{"US", "JP"}, []int64{10, 30})
	// keys: 1 match, 2 no, 3 match, 4 no. amt = 1,2,3,4.
	probe := probeInt([]int64{1, 2, 3, 4}, []int64{1, 2, 3, 4})
	c := vector.Collect()
	err := sdk.New().
		From(vector.MemSource([]*core.Batch{probe})).
		Apply(vector.HashJoin([]*core.Batch{dim}, "id", "user_id").Bring("country", "country").LeftOuter().Op()).
		Apply(vector.FilterInt64("amt", func(x int64) bool { return x%2 == 0 })). // keep amt 2,4 (both unmatched)
		To(c).Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	res := c.Batches()[0]
	if res.Len != 2 {
		t.Fatalf("after filter rows = %d, want 2", res.Len)
	}
	cNull := res.IsNull("country")
	uid := res.Int64("user_id")
	// both survivors (uid 2 and 4) were unmatched → NULL country.
	for i := 0; i < res.Len; i++ {
		if !cNull[i] {
			t.Fatalf("row %d uid%d: expected NULL country after compaction", i, uid[i])
		}
	}
}

// TestHashJoin_LeftOuter_ToRows checks NULL cells surface as nil on the row path.
func TestHashJoin_LeftOuter_ToRows(t *testing.T) {
	dim := dimInt([]int64{1}, []string{"US"}, []int64{10})
	probe := probeInt([]int64{1, 99}, []int64{5, 6})
	out := sdk.Collect()
	err := sdk.New().
		From(vector.MemSource([]*core.Batch{probe})).
		Apply(vector.HashJoin([]*core.Batch{dim}, "id", "user_id").Bring("country", "country").LeftOuter().Op()).
		Apply(vector.ToRows()).
		To(out).Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	rows := out.Records()
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	// matched row has a country; unmatched has nil.
	if rows[0].Payload["country"] != "US" {
		t.Fatalf("row0 country = %v, want US", rows[0].Payload["country"])
	}
	if v, ok := rows[1].Payload["country"]; !ok || v != nil {
		t.Fatalf("row1 country = %v (ok=%v), want explicit nil", v, ok)
	}
}

// TestEncodeBatch_NullRefused guards against silently dropping NULLs over the wire.
func TestEncodeBatch_NullRefused(t *testing.T) {
	b := &core.Batch{
		Schema: core.Schema{Fields: []core.Field{{Name: "x", Type: core.FieldTypeInt}}},
		Len:    2,
		Cols:   []core.Column{{Kind: core.KindInt64, I64: []int64{1, 0}, Null: []bool{false, true}}},
	}
	if _, err := vector.EncodeBatch(b); err == nil {
		t.Fatal("expected EncodeBatch to refuse a column with NULLs")
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
