package core_test

import (
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
)

func TestBatch_StringBoolAccessors(t *testing.T) {
	b := &core.Batch{
		Schema: core.Schema{Fields: []core.Field{
			{Name: "s", Type: core.FieldTypeString},
			{Name: "ok", Type: core.FieldTypeBool},
		}},
		Len: 3,
		Cols: []core.Column{
			{Kind: core.KindString, Str: []string{"a", "b", "c"}},
			{Kind: core.KindBool, B: []bool{true, false, true}},
		},
	}
	if got := b.String("s"); len(got) != 3 || got[1] != "b" {
		t.Fatalf("String(s) = %v", got)
	}
	if got := b.Bool("ok"); len(got) != 3 || got[0] != true || got[1] != false {
		t.Fatalf("Bool(ok) = %v", got)
	}
	if b.String("ok") != nil || b.Bool("s") != nil || b.String("missing") != nil {
		t.Fatal("wrong-kind / missing access should be nil")
	}
}
