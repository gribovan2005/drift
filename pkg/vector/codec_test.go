package vector_test

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/vector"
)

func TestCodec_RoundTrip(t *testing.T) {
	in := &core.Batch{
		Schema: core.Schema{Fields: []core.Field{
			{Name: "v", Type: core.FieldTypeInt},
			{Name: "f", Type: core.FieldTypeFloat},
		}},
		Len: 3,
		Cols: []core.Column{
			{Kind: core.KindInt64, I64: []int64{-5, 0, 42}},
			{Kind: core.KindFloat64, F64: []float64{1.5, -2.25, 3.75}},
		},
	}
	enc, err := vector.EncodeBatch(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := vector.DecodeBatch(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Len != 3 {
		t.Fatalf("len = %d, want 3", out.Len)
	}
	if v := out.Int64("v"); v[0] != -5 || v[1] != 0 || v[2] != 42 {
		t.Fatalf("int col = %v", v)
	}
	if f := out.Float64("f"); f[0] != 1.5 || f[1] != -2.25 || f[2] != 3.75 {
		t.Fatalf("float col = %v", f)
	}
}

func TestCodec_RoundTrip_StringBool(t *testing.T) {
	in := &core.Batch{
		Schema: core.Schema{Fields: []core.Field{
			{Name: "cat", Type: core.FieldTypeString},
			{Name: "ok", Type: core.FieldTypeBool},
			{Name: "v", Type: core.FieldTypeInt},
		}},
		Len: 3,
		Cols: []core.Column{
			{Kind: core.KindString, Str: []string{"alpha", "", "héllo"}}, // incl. empty + multibyte
			{Kind: core.KindBool, B: []bool{true, false, true}},
			{Kind: core.KindInt64, I64: []int64{7, 8, 9}},
		},
	}
	enc, err := vector.EncodeBatch(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := vector.DecodeBatch(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if s := out.String("cat"); s[0] != "alpha" || s[1] != "" || s[2] != "héllo" {
		t.Fatalf("string col = %v", s)
	}
	if b := out.Bool("ok"); b[0] != true || b[1] != false || b[2] != true {
		t.Fatalf("bool col = %v", b)
	}
	if v := out.Int64("v"); v[2] != 9 {
		t.Fatalf("int col = %v", v)
	}
}

func TestCodec_TruncatedFrame(t *testing.T) {
	if _, err := vector.DecodeBatch([]byte{1, 2}); err == nil {
		t.Fatal("expected error on short frame")
	}
}

func TestBinSource_DecodesThroughPipeline(t *testing.T) {
	batches := vector.GenInt64("v", 4, 50, func(i int) int64 { return int64(i) })
	frames := make([][]byte, len(batches))
	for i, b := range batches {
		enc, err := vector.EncodeBatch(b)
		if err != nil {
			t.Fatal(err)
		}
		frames[i] = enc
	}
	c := vector.Collect()
	src := vector.BinSource(frames)
	ch, err := src.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// drain into collector manually (source → sink shape)
	if err := c.Write(context.Background(), ch); err != nil {
		t.Fatal(err)
	}
	if c.Rows() != 200 {
		t.Fatalf("rows = %d, want 200", c.Rows())
	}
}
