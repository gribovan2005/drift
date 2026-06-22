package sdk_test

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/sdk"
)

func TestRunLanes_Union(t *testing.T) {
	c0, c1, c2 := sdk.Collect(), sdk.Collect(), sdk.Collect()
	err := sdk.RunLanes(context.Background(),
		sdk.New().From(sdk.Slice(recs(40))).To(c0),
		sdk.New().From(sdk.Slice(recs(60))).To(c1),
		sdk.New().From(sdk.Slice(recs(50))).To(c2),
	)
	if err != nil {
		t.Fatalf("run lanes: %v", err)
	}
	total := len(c0.Records()) + len(c1.Records()) + len(c2.Records())
	if total != 150 {
		t.Fatalf("union = %d, want 150", total)
	}
}

func TestRunLanes_BuildError(t *testing.T) {
	// second lane has no sink → Build fails, surfaced before running.
	err := sdk.RunLanes(context.Background(),
		sdk.New().From(sdk.Slice(recs(10))).To(sdk.Discard()),
		sdk.New().From(sdk.Slice(recs(10))), // missing To
	)
	if err == nil {
		t.Fatal("expected build error for lane missing a sink")
	}
}
