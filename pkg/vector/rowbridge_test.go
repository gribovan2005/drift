package vector_test

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/sink"
	"github.com/gribovan2005/drift/pkg/source"
	"github.com/gribovan2005/drift/pkg/vector"
)

// TestFromRows_InferAndBatch: rows → chunks of `size`, schema inferred (sorted names,
// Go-typed), partial final chunk flushed; round-trips back through ToRows.
func TestFromRows_InferAndBatch(t *testing.T) {
	rows := make([]core.Record, 5)
	for i := range rows {
		rows[i] = core.Record{Payload: map[string]any{"k": int64(i), "f": float64(i) + 0.5}}
	}
	c := vector.Collect()
	p := pipeline.New(source.NewMemory(rows),
		[]pipeline.Stage{{Label: "b", Op: vector.FromRows(2)}}, c)
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	batches := c.Batches()
	// 5 rows, size 2 → chunks of 2,2,1.
	if len(batches) != 3 {
		t.Fatalf("got %d chunks, want 3", len(batches))
	}
	// Inferred schema: sorted names → ["f","k"], typed Float64/Int64.
	f := batches[0].Schema.Fields
	if len(f) != 2 || f[0].Name != "f" || f[0].Type != core.FieldTypeFloat || f[1].Name != "k" || f[1].Type != core.FieldTypeInt {
		t.Fatalf("inferred schema wrong: %+v", f)
	}
	var total int
	var sumK int64
	for _, b := range batches {
		kc := b.Int64("k")
		total += b.Len
		for _, v := range kc {
			sumK += v
		}
	}
	if total != 5 || sumK != 0+1+2+3+4 {
		t.Fatalf("total=%d sumK=%d, want 5 and 10", total, sumK)
	}
}

// TestFromRows_MissingFieldIsNull: a row missing an inferred field gets a NULL cell.
func TestFromRows_MissingFieldIsNull(t *testing.T) {
	rows := []core.Record{
		{Payload: map[string]any{"a": int64(1), "b": int64(2)}},
		{Payload: map[string]any{"a": int64(3)}}, // missing "b" → NULL
	}
	c := vector.Collect()
	p := pipeline.New(source.NewMemory(rows),
		[]pipeline.Stage{{Label: "b", Op: vector.FromRows(8)}}, c)
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	b := c.Batches()[0]
	bNull := b.IsNull("b")
	if bNull == nil {
		t.Fatal("expected null mask on column b")
	}
	if bNull[0] || !bNull[1] {
		t.Fatalf("null mask = %v, want [false true]", bNull)
	}
}

// TestFromRows_RoundTrip: FromRows → ToRows reconstructs the rows.
func TestFromRows_RoundTrip(t *testing.T) {
	rows := []core.Record{
		{Payload: map[string]any{"name": "x", "n": int64(7)}},
		{Payload: map[string]any{"name": "y", "n": int64(8)}},
	}
	out := sink.NewMemory()
	p := pipeline.New(source.NewMemory(rows),
		[]pipeline.Stage{
			{Label: "b", Op: vector.FromRows(0)}, // default size
			{Label: "r", Op: vector.ToRows()},
		}, out)
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := out.Records()
	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2", len(got))
	}
	seen := map[string]int64{}
	for _, r := range got {
		seen[r.Payload["name"].(string)] = r.Payload["n"].(int64)
	}
	if seen["x"] != 7 || seen["y"] != 8 {
		t.Fatalf("round-trip mismatch: %v", seen)
	}
}
