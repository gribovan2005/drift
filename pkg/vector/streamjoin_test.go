package vector_test

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/vector"
)

// sideBatch builds a join-side chunk tagged with Schema.ID: int64 key + int64 "ts" +
// one int64 value column.
func sideBatch(id, keyField string, keys, ts []int64, valField string, vals []int64) *core.Batch {
	return &core.Batch{
		Schema: core.Schema{ID: id, Fields: []core.Field{
			{Name: keyField, Type: core.FieldTypeInt},
			{Name: "ts", Type: core.FieldTypeInt},
			{Name: valField, Type: core.FieldTypeInt},
		}},
		Len: len(keys),
		Cols: []core.Column{
			{Kind: core.KindInt64, I64: keys},
			{Kind: core.KindInt64, I64: ts},
			{Kind: core.KindInt64, I64: vals},
		},
	}
}

type sjRow struct {
	oid, ts, amt, tsR, qty int64
}

func flattenSJ(t *testing.T, c *vector.Collector) []sjRow {
	t.Helper()
	var out []sjRow
	for _, b := range c.Batches() {
		oid, ts, amt, tsR, qty := b.Int64("oid"), b.Int64("ts"), b.Int64("amt"), b.Int64("ts_r"), b.Int64("qty")
		if oid == nil || ts == nil || amt == nil || tsR == nil || qty == nil {
			t.Fatalf("output missing expected columns: %+v", b.Schema.Fields)
		}
		for i := 0; i < b.Len; i++ {
			out = append(out, sjRow{oid[i], ts[i], amt[i], tsR[i], qty[i]})
		}
	}
	return out
}

func TestStreamJoin_IntervalMatch(t *testing.T) {
	left := sideBatch("L", "oid", []int64{1, 2}, []int64{100, 200}, "amt", []int64{10, 20})
	right := sideBatch("R", "oid", []int64{1, 2}, []int64{150, 260}, "qty", []int64{5, 6})
	c := runVec(t, []*core.Batch{left, right},
		vector.StreamJoin("L", "R", "oid", "ts", 60).Op())

	rows := flattenSJ(t, c)
	if len(rows) != 2 {
		t.Fatalf("got %d matches, want 2: %+v", len(rows), rows)
	}
	// oid1: |150-100|=50≤60 ; oid2: |260-200|=60≤60.
	want := map[int64]sjRow{
		1: {1, 100, 10, 150, 5},
		2: {2, 200, 20, 260, 6},
	}
	for _, r := range rows {
		if r != want[r.oid] {
			t.Fatalf("oid %d: got %+v, want %+v", r.oid, r, want[r.oid])
		}
	}
	// Output schema: right key dropped, right ts renamed ts_r.
	f := c.Batches()[0].Schema.Fields
	names := []string{f[0].Name, f[1].Name, f[2].Name, f[3].Name, f[4].Name}
	wantNames := []string{"oid", "ts", "amt", "ts_r", "qty"}
	for i := range wantNames {
		if names[i] != wantNames[i] {
			t.Fatalf("output fields = %v, want %v", names, wantNames)
		}
	}
}

func TestStreamJoin_OutsideWindowNoMatch(t *testing.T) {
	left := sideBatch("L", "oid", []int64{1}, []int64{100}, "amt", []int64{10})
	right := sideBatch("R", "oid", []int64{1}, []int64{200}, "qty", []int64{5}) // |200-100|=100>60
	c := runVec(t, []*core.Batch{left, right},
		vector.StreamJoin("L", "R", "oid", "ts", 60).Op())
	if rows := flattenSJ(t, c); len(rows) != 0 {
		t.Fatalf("expected no match (outside window), got %+v", rows)
	}
}

func TestStreamJoin_SymmetricRightFirst(t *testing.T) {
	// Right arrives first (buffered), then left matches it.
	right := sideBatch("R", "oid", []int64{7}, []int64{1000}, "qty", []int64{9})
	left := sideBatch("L", "oid", []int64{7}, []int64{1040}, "amt", []int64{3}) // |1040-1000|=40≤60
	c := runVec(t, []*core.Batch{right, left},
		vector.StreamJoin("L", "R", "oid", "ts", 60).Op())
	rows := flattenSJ(t, c)
	if len(rows) != 1 || rows[0] != (sjRow{7, 1040, 3, 1000, 9}) {
		t.Fatalf("got %+v, want one row {7 1040 3 1000 9}", rows)
	}
}

func TestStreamJoin_ManyToMany(t *testing.T) {
	// Two left rows for oid 1 within window of one right row → 2 matches.
	left := sideBatch("L", "oid", []int64{1, 1}, []int64{100, 130}, "amt", []int64{10, 11})
	right := sideBatch("R", "oid", []int64{1}, []int64{150}, "qty", []int64{5}) // matches both (50,20 ≤60)
	c := runVec(t, []*core.Batch{left, right},
		vector.StreamJoin("L", "R", "oid", "ts", 60).Op())
	rows := flattenSJ(t, c)
	if len(rows) != 2 {
		t.Fatalf("got %d matches, want 2 (M:N): %+v", len(rows), rows)
	}
	sum := rows[0].amt + rows[1].amt
	if sum != 21 || rows[0].qty != 5 || rows[1].qty != 5 {
		t.Fatalf("unexpected M:N rows: %+v", rows)
	}
}

func TestStreamJoin_LateDropByWatermark(t *testing.T) {
	// Right at ts=1000 advances the watermark; a later left at ts=100 is too old to
	// match (100 < wm 1000 − window 60) and is dropped.
	right := sideBatch("R", "oid", []int64{1}, []int64{1000}, "qty", []int64{5})
	left := sideBatch("L", "oid", []int64{1}, []int64{100}, "amt", []int64{10})
	c := runVec(t, []*core.Batch{right, left},
		vector.StreamJoin("L", "R", "oid", "ts", 60).Op())
	if rows := flattenSJ(t, c); len(rows) != 0 {
		t.Fatalf("expected late left row dropped, got %+v", rows)
	}
}

func TestStreamJoin_StringKey(t *testing.T) {
	mk := func(id, valField string, key string, ts int64, val int64) *core.Batch {
		return &core.Batch{
			Schema: core.Schema{ID: id, Fields: []core.Field{
				{Name: "oid", Type: core.FieldTypeString},
				{Name: "ts", Type: core.FieldTypeInt},
				{Name: valField, Type: core.FieldTypeInt},
			}},
			Len: 1,
			Cols: []core.Column{
				{Kind: core.KindString, Str: []string{key}},
				{Kind: core.KindInt64, I64: []int64{ts}},
				{Kind: core.KindInt64, I64: []int64{val}},
			},
		}
	}
	c := runVec(t, []*core.Batch{mk("L", "amt", "x", 100, 10), mk("R", "qty", "x", 120, 5)},
		vector.StreamJoin("L", "R", "oid", "ts", 60).Op())
	var n int
	for _, b := range c.Batches() {
		key := b.String("oid")
		if key == nil || (b.Len > 0 && key[0] != "x") {
			t.Fatalf("string key join output wrong: %v", key)
		}
		n += b.Len
	}
	if n != 1 {
		t.Fatalf("string-key join matches = %d, want 1", n)
	}
}

func TestStreamJoin_Errors(t *testing.T) {
	run := func(op core.Operator, batches []*core.Batch) error {
		p := pipeline.New(vector.MemSource(batches), []pipeline.Stage{{Label: "j", Op: op}}, vector.Discard())
		return p.Run(context.Background())
	}
	l := sideBatch("L", "oid", []int64{1}, []int64{1}, "amt", []int64{1})
	// same left/right ID
	if err := run(vector.StreamJoin("X", "X", "oid", "ts", 10).Op(), []*core.Batch{l}); err == nil {
		t.Fatal("expected error for identical left/right Schema.ID")
	}
	// missing ts column
	noTs := &core.Batch{
		Schema: core.Schema{ID: "L", Fields: []core.Field{{Name: "oid", Type: core.FieldTypeInt}}},
		Len:    1, Cols: []core.Column{{Kind: core.KindInt64, I64: []int64{1}}},
	}
	if err := run(vector.StreamJoin("L", "R", "oid", "ts", 10).Op(), []*core.Batch{noTs}); err == nil {
		t.Fatal("expected error for missing ts column")
	}
}
