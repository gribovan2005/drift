package core

import "testing"

func TestBatchClone_DeepIndependent(t *testing.T) {
	orig := &Batch{
		Schema: Schema{Fields: []Field{
			{Name: "i", Type: FieldTypeInt},
			{Name: "f", Type: FieldTypeFloat},
			{Name: "s", Type: FieldTypeString},
			{Name: "b", Type: FieldTypeBool},
		}},
		Len: 2,
		Cols: []Column{
			{Kind: KindInt64, I64: []int64{1, 2}, Null: []bool{false, true}},
			{Kind: KindFloat64, F64: []float64{1.5, 2.5}},
			{Kind: KindString, Str: []string{"a", "b"}},
			{Kind: KindBool, B: []bool{true, false}},
		},
	}
	cl := orig.Clone()

	// Mutate every part of the clone, incl. appending a field/column (as join does).
	cl.Cols[0].I64[0] = 99
	cl.Cols[0].Null[0] = true
	cl.Cols[1].F64[1] = 9.9
	cl.Cols[2].Str[0] = "z"
	cl.Cols[3].B[0] = false
	cl.Schema.Fields = append(cl.Schema.Fields, Field{Name: "x", Type: FieldTypeInt})
	cl.Cols = append(cl.Cols, Column{Kind: KindInt64, I64: []int64{7, 8}})
	cl.Len = 5

	// Original must be untouched.
	if orig.Cols[0].I64[0] != 1 || orig.Cols[0].Null[0] != false {
		t.Fatalf("orig int col mutated: %+v", orig.Cols[0])
	}
	if orig.Cols[1].F64[1] != 2.5 {
		t.Fatalf("orig float col mutated: %v", orig.Cols[1].F64)
	}
	if orig.Cols[2].Str[0] != "a" {
		t.Fatalf("orig string col mutated: %v", orig.Cols[2].Str)
	}
	if orig.Cols[3].B[0] != true {
		t.Fatalf("orig bool col mutated: %v", orig.Cols[3].B)
	}
	if len(orig.Schema.Fields) != 4 || len(orig.Cols) != 4 || orig.Len != 2 {
		t.Fatalf("orig shape mutated: fields=%d cols=%d len=%d", len(orig.Schema.Fields), len(orig.Cols), orig.Len)
	}
}

func TestBatchClone_Nil(t *testing.T) {
	var b *Batch
	if b.Clone() != nil {
		t.Fatal("nil.Clone() should be nil")
	}
}
