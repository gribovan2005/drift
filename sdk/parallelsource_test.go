package sdk_test

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/sdk"
)

func TestParallelSource_EndToEnd(t *testing.T) {
	a := make([]sdk.Record, 50)
	b := make([]sdk.Record, 70)
	for i := range a {
		a[i] = sdk.Record{Payload: map[string]any{"v": i}}
	}
	for i := range b {
		b[i] = sdk.Record{Payload: map[string]any{"v": 1000 + i}}
	}

	c := sdk.Collect()
	err := sdk.New().
		From(sdk.ParallelSource(sdk.Slice(a), sdk.Slice(b))).
		To(c).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(c.Records()) != 120 {
		t.Fatalf("got %d records, want 120 (fan-in of 50+70)", len(c.Records()))
	}
}
