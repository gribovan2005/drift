package sdk_test

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/sdk"
)

// TestBranch_TeeAllRecords: two branches partition the input (even/odd); their union
// is the whole input exactly once.
func TestBranch_TeeAllRecords(t *testing.T) {
	out := sdk.Collect()
	err := sdk.New().
		From(sdk.Slice(recs(10))).
		Branch(
			func(b *sdk.Branch) { b.Filter(func(r sdk.Record) bool { return r.Payload["v"].(int)%2 == 0 }) },
			func(b *sdk.Branch) { b.Filter(func(r sdk.Record) bool { return r.Payload["v"].(int)%2 == 1 }) },
		).
		To(out).Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	seen := map[int]int{}
	for _, r := range out.Records() {
		seen[r.Payload["v"].(int)]++
	}
	if len(seen) != 10 {
		t.Fatalf("distinct values = %d, want 10: %v", len(seen), seen)
	}
	for i := 0; i < 10; i++ {
		if seen[i] != 1 {
			t.Fatalf("value %d seen %d times, want 1", i, seen[i])
		}
	}
}

// TestBranch_PrefixThenIndependentMaps: a prefix Map runs once before the split, then
// each branch transforms independently (no cross-branch corruption thanks to per-branch
// Payload copy).
func TestBranch_PrefixThenIndependentMaps(t *testing.T) {
	out := sdk.Collect()
	err := sdk.New().
		From(sdk.Slice(recs(4))). // v = 0,1,2,3
		Map(func(r sdk.Record) (sdk.Record, error) {
			r.Payload["v"] = r.Payload["v"].(int) + 1 // → 1,2,3,4
			return r, nil
		}).
		Branch(
			func(b *sdk.Branch) {
				b.Map(func(r sdk.Record) (sdk.Record, error) {
					r.Payload["v"] = r.Payload["v"].(int) * 10
					return r, nil
				})
			},
			func(b *sdk.Branch) {
				b.Map(func(r sdk.Record) (sdk.Record, error) {
					r.Payload["v"] = r.Payload["v"].(int) * 100
					return r, nil
				})
			},
		).
		To(out).Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	var sum int
	n := 0
	for _, r := range out.Records() {
		sum += r.Payload["v"].(int)
		n++
	}
	// branch A: (1+2+3+4)*10 = 100 ; branch B: (1+2+3+4)*100 = 1000 ; total 1100, 8 rows.
	if n != 8 || sum != 1100 {
		t.Fatalf("got n=%d sum=%d, want n=8 sum=1100 (branches corrupted each other?)", n, sum)
	}
}

// TestBranch_NoPrefixRoots: branching right after From (source fans out to the branch
// heads as roots).
func TestBranch_NoPrefixRoots(t *testing.T) {
	out := sdk.Collect()
	err := sdk.New().
		From(sdk.Slice(recs(6))).
		Branch(
			func(b *sdk.Branch) { b.Filter(func(r sdk.Record) bool { return r.Payload["v"].(int) < 3 }) },
			func(b *sdk.Branch) { b.Filter(func(r sdk.Record) bool { return r.Payload["v"].(int) >= 3 }) },
		).
		To(out).Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := len(out.Records()); got != 6 {
		t.Fatalf("total = %d, want 6", got)
	}
}

// TestBranch_EmptyBranchTees: an empty branch passes its input straight through.
func TestBranch_EmptyBranchTees(t *testing.T) {
	out := sdk.Collect()
	err := sdk.New().
		From(sdk.Slice(recs(5))).
		Branch(
			func(b *sdk.Branch) {}, // tee
			func(b *sdk.Branch) { b.Filter(func(sdk.Record) bool { return false }) }, // drops all
		).
		To(out).Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := len(out.Records()); got != 5 {
		t.Fatalf("tee branch should pass 5 records, got %d", got)
	}
}

func TestBranch_Errors(t *testing.T) {
	// < 2 branches
	err := sdk.New().From(sdk.Slice(recs(1))).
		Branch(func(b *sdk.Branch) { b.Filter(func(sdk.Record) bool { return true }) }).
		To(sdk.Collect()).Run(context.Background())
	if err == nil {
		t.Fatal("expected error for Branch with <2 branches")
	}

	// linear stage after Branch
	err = sdk.New().From(sdk.Slice(recs(1))).
		Branch(
			func(b *sdk.Branch) { b.Filter(func(sdk.Record) bool { return true }) },
			func(b *sdk.Branch) { b.Filter(func(sdk.Record) bool { return true }) },
		).
		Map(func(r sdk.Record) (sdk.Record, error) { return r, nil }). // illegal after Branch
		To(sdk.Collect()).Run(context.Background())
	if err == nil {
		t.Fatal("expected error appending a linear stage after Branch")
	}
}

// TestBranch_GraphIsDAG verifies the built pipeline graph reflects the fan-out.
func TestBranch_GraphIsDAG(t *testing.T) {
	p, err := sdk.New().
		From(sdk.Slice(recs(2))).
		Map(func(r sdk.Record) (sdk.Record, error) { return r, nil }).
		Branch(
			func(b *sdk.Branch) { b.Filter(func(sdk.Record) bool { return true }) },
			func(b *sdk.Branch) { b.Filter(func(sdk.Record) bool { return true }) },
		).
		To(sdk.Collect()).Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	nodes := p.Graph()
	var fanout int
	for _, n := range nodes {
		if len(n.Next) >= 2 {
			fanout++
		}
	}
	if fanout != 1 {
		t.Fatalf("expected exactly one fan-out node, got %d (nodes=%+v)", fanout, nodes)
	}
}
