package integration

import (
	"context"
	"testing"
	"time"

	"github.com/andrejgribov/drift/pkg/core"
	"github.com/andrejgribov/drift/pkg/operator"
	"github.com/andrejgribov/drift/pkg/pipeline"
	"github.com/andrejgribov/drift/pkg/sink"
	"github.com/andrejgribov/drift/pkg/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func idRec(id string) core.Record {
	return core.Record{Payload: map[string]any{"id": id}}
}

func keyByID(r core.Record) string {
	v, _ := r.Payload["id"].(string)
	return v
}

// TestPipeline_Deduplicate_DropsWithinWindow verifies that a duplicate key
// within the window is dropped and only 2 distinct records reach the sink.
func TestPipeline_Deduplicate_DropsWithinWindow(t *testing.T) {
	recs := []core.Record{idRec("a"), idRec("b"), idRec("a")}
	src := source.NewMemory(recs)
	snk := sink.NewMemory()

	dedup := operator.NewDeduplicate(keyByID, time.Hour)
	p := pipeline.New(src, []pipeline.Stage{{Label: "dedup", Op: dedup}}, snk)
	require.NoError(t, p.Run(context.Background()))

	got := snk.Records()
	assert.Len(t, got, 2)
	ids := make([]string, len(got))
	for i, r := range got {
		ids[i], _ = r.Payload["id"].(string)
	}
	assert.ElementsMatch(t, []string{"a", "b"}, ids)
}

// TestMerge_Standalone verifies that Merge combines primary and extra records.
func TestMerge_Standalone(t *testing.T) {
	extra := make(chan []core.Record, 1)
	extra <- []core.Record{idRec("extra")}

	src := source.NewMemory([]core.Record{idRec("primary")})
	snk := sink.NewMemory()

	m := operator.NewMerge(extra)
	p := pipeline.New(src, []pipeline.Stage{{Label: "merge", Op: m}}, snk)
	require.NoError(t, p.Run(context.Background()))

	got := snk.Records()
	assert.Len(t, got, 2)
	ids := make([]string, len(got))
	for i, r := range got {
		ids[i], _ = r.Payload["id"].(string)
	}
	assert.ElementsMatch(t, []string{"primary", "extra"}, ids)
}

// TestSplit_Standalone verifies that Split routes records to primary and a
// side channel, and that both sets are correct.
func TestSplit_Standalone(t *testing.T) {
	recs := []core.Record{
		{Payload: map[string]any{"n": 1}},
		{Payload: map[string]any{"n": 2}},
		{Payload: map[string]any{"n": 3}},
		{Payload: map[string]any{"n": 4}},
	}
	src := source.NewMemory(recs)
	snk := sink.NewMemory()

	// Odd → route 0 (primary sink), Even → route 1 (side channel)
	sp, err := operator.NewSplit(2, func(r core.Record) int {
		n, _ := r.Payload["n"].(int)
		if n%2 == 0 {
			return 1
		}
		return 0
	}, 100)
	require.NoError(t, err)

	// Drain the side channel concurrently.
	var sideRecs []core.Record
	sideDone := make(chan struct{})
	go func() {
		defer close(sideDone)
		for r := range sp.Outputs()[0] {
			sideRecs = append(sideRecs, r)
		}
	}()

	p := pipeline.New(src, []pipeline.Stage{{Label: "split", Op: sp}}, snk)
	require.NoError(t, p.Run(context.Background()))
	sp.Close()
	<-sideDone

	// Primary: odd numbers
	got := snk.Records()
	assert.Len(t, got, 2)
	for _, r := range got {
		n, _ := r.Payload["n"].(int)
		assert.True(t, n%2 != 0, "expected odd in primary, got %d", n)
	}

	// Side channel: even numbers
	assert.Len(t, sideRecs, 2)
	for _, r := range sideRecs {
		n, _ := r.Payload["n"].(int)
		assert.True(t, n%2 == 0, "expected even in side channel, got %d", n)
	}
}
