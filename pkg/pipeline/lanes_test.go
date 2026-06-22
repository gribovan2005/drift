package pipeline_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/operator"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/sink"
	"github.com/gribovan2005/drift/pkg/source"
)

func recsRange(lo, hi int) []core.Record {
	out := make([]core.Record, 0, hi-lo)
	for i := lo; i < hi; i++ {
		out = append(out, core.Record{Payload: map[string]any{"v": i}})
	}
	return out
}

func identity() pipeline.Stage {
	return pipeline.Stage{Label: "id", Op: operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil })}
}

func TestRunLanes_AllProcessed(t *testing.T) {
	// 3 lanes over disjoint shards → each sink gets its shard; union = 300.
	shards := [][2]int{{0, 100}, {100, 250}, {250, 300}}
	sinks := make([]*sink.Memory, len(shards))
	lanes := make([]*pipeline.Pipeline, len(shards))
	for i, s := range shards {
		sinks[i] = sink.NewMemory()
		lanes[i] = pipeline.New(source.NewMemory(recsRange(s[0], s[1])), []pipeline.Stage{identity()}, sinks[i])
	}
	if err := pipeline.RunLanes(context.Background(), lanes...); err != nil {
		t.Fatalf("run lanes: %v", err)
	}
	total := 0
	seen := map[int]int{}
	for _, sk := range sinks {
		for _, r := range sk.Records() {
			seen[r.Payload["v"].(int)]++
			total++
		}
	}
	if total != 300 {
		t.Fatalf("total = %d, want 300", total)
	}
	for i := 0; i < 300; i++ {
		if seen[i] != 1 {
			t.Fatalf("value %d seen %d times, want 1", i, seen[i])
		}
	}
}

func TestRunLanes_FailFast(t *testing.T) {
	// Lane A errors; lane B has a never-closing source → must be cancelled, no hang.
	bad := pipeline.New(
		source.NewMemory(recsRange(0, 10)),
		[]pipeline.Stage{{Label: "boom", Op: operator.NewMap(func(core.Record) (core.Record, error) {
			return core.Record{}, errors.New("boom")
		})}},
		sink.NewMemory(),
	)
	gen := source.NewGenerator(func(seq int) core.Record { return core.Record{Payload: map[string]any{"v": seq}} }, time.Millisecond)
	live := pipeline.New(gen, []pipeline.Stage{identity()}, sink.NewMemory())

	done := make(chan error, 1)
	go func() { done <- pipeline.RunLanes(context.Background(), bad, live) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error from failing lane")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunLanes hung — sibling lane not cancelled on failure")
	}
}

func TestRunLanes_CtxCancel(t *testing.T) {
	gen := source.NewGenerator(func(seq int) core.Record { return core.Record{Payload: map[string]any{"v": seq}} }, time.Millisecond)
	lane := pipeline.New(gen, []pipeline.Stage{identity()}, sink.NewMemory())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pipeline.RunLanes(ctx, lane) }()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunLanes did not return after ctx cancel")
	}
}
