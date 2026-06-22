package core_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
)

func twoColBatch() *core.Batch {
	return &core.Batch{
		Schema: core.Schema{Fields: []core.Field{
			{Name: "v", Type: core.FieldTypeInt},
			{Name: "f", Type: core.FieldTypeFloat},
		}},
		Len: 4,
		Cols: []core.Column{
			{Kind: core.KindInt64, I64: []int64{10, 11, 12, 13}},
			{Kind: core.KindFloat64, F64: []float64{1.5, 2.5, 3.5, 4.5}},
		},
	}
}

func TestBatch_Accessors(t *testing.T) {
	b := twoColBatch()
	if got := b.Int64("v"); len(got) != 4 || got[2] != 12 {
		t.Fatalf("Int64(v) = %v", got)
	}
	if got := b.Float64("f"); len(got) != 4 || got[1] != 2.5 {
		t.Fatalf("Float64(f) = %v", got)
	}
	if b.Int64("missing") != nil {
		t.Fatal("missing field should be nil")
	}
	if b.Int64("f") != nil {
		t.Fatal("wrong-kind access (Int64 on float col) should be nil")
	}
	if b.Float64("v") != nil {
		t.Fatal("wrong-kind access (Float64 on int col) should be nil")
	}
}

func TestBatch_CopyRowAndTruncate(t *testing.T) {
	b := twoColBatch()
	// Simulate compaction: keep rows 1 and 3 (move them to front).
	b.CopyRow(0, 1)
	b.CopyRow(1, 3)
	b.Truncate(2)
	if b.Len != 2 {
		t.Fatalf("Len = %d, want 2", b.Len)
	}
	if v := b.Int64("v"); v[0] != 11 || v[1] != 13 {
		t.Fatalf("int col after compaction = %v, want [11 13]", v)
	}
	if f := b.Float64("f"); f[0] != 2.5 || f[1] != 4.5 {
		t.Fatalf("float col after compaction = %v, want [2.5 4.5]", f)
	}
}

func TestRecord_ChunkJSONOmitEmpty(t *testing.T) {
	// A normal row record must marshal without a "chunk" key (wire format unchanged).
	row := core.Record{Payload: map[string]any{"v": 1}}
	out, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "chunk") {
		t.Fatalf("row record JSON should not contain chunk: %s", out)
	}
	// A chunk-record does carry it.
	chunk := core.Record{Chunk: twoColBatch()}
	out2, _ := json.Marshal(chunk)
	if !strings.Contains(string(out2), "chunk") {
		t.Fatalf("chunk record JSON should contain chunk: %s", out2)
	}
}
