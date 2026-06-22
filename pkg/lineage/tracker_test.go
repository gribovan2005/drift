package lineage_test

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/lineage"
	"github.com/gribovan2005/drift/pkg/operator"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func recs(vals ...int) []core.Record {
	out := make([]core.Record, len(vals))
	for i, v := range vals {
		out[i] = core.Record{SchemaID: "events", SchemaVersion: 1, Payload: map[string]any{"v": v}}
	}
	return out
}

func TestTracker_AssignsRootIDs(t *testing.T) {
	tr := lineage.New()
	op := tr.Wrap("identity", operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil }))

	out, err := op.Process(recs(1, 2, 3))
	require.NoError(t, err)
	require.Len(t, out, 3)

	// 3 root nodes + 3 output nodes.
	assert.Equal(t, 6, tr.Len())
	for _, o := range out {
		assert.NotEmpty(t, o.ID)
		require.Len(t, o.Parents, 1)
		root, ok := tr.Get(o.Parents[0])
		require.True(t, ok)
		assert.Equal(t, "source", root.Stage)
		assert.Equal(t, "events", root.SchemaID)
	}
}

func TestTracker_MapExactParent(t *testing.T) {
	tr := lineage.New()
	op := tr.Wrap("double", operator.NewMap(func(r core.Record) (core.Record, error) {
		r.Payload["v"] = r.Payload["v"].(int) * 2
		return r, nil
	}))

	out, err := op.Process(recs(5))
	require.NoError(t, err)
	require.Len(t, out, 1)

	node, ok := tr.Get(out[0].ID)
	require.True(t, ok)
	assert.Equal(t, "double", node.Stage)
	require.Len(t, node.Parents, 1)
	// Parent is a root node, exactly one input.
	parent, ok := tr.Get(node.Parents[0])
	require.True(t, ok)
	assert.Equal(t, "source", parent.Stage)
}

func TestTracker_FilterDropsHaveNoNode(t *testing.T) {
	tr := lineage.New()
	op := tr.Wrap("evens", operator.NewFilter(func(r core.Record) bool {
		return r.Payload["v"].(int)%2 == 0
	}))

	out, err := op.Process(recs(1, 2, 3, 4))
	require.NoError(t, err)
	require.Len(t, out, 2) // 2 and 4 survive

	// 4 roots + 2 surviving output nodes.
	assert.Equal(t, 6, tr.Len())
	for _, o := range out {
		assert.Equal(t, 0, o.Payload["v"].(int)%2)
	}
}

func TestTracker_FlatMapMultipleChildren(t *testing.T) {
	tr := lineage.New()
	op := tr.Wrap("fanout", operator.NewFlatMap(func(r core.Record) ([]core.Record, error) {
		return []core.Record{r, r}, nil // each input yields two children
	}))

	out, err := op.Process(recs(7))
	require.NoError(t, err)
	require.Len(t, out, 2)

	// Both children share the same single parent.
	require.Len(t, out[0].Parents, 1)
	require.Len(t, out[1].Parents, 1)
	assert.Equal(t, out[0].Parents[0], out[1].Parents[0])
	assert.NotEqual(t, out[0].ID, out[1].ID)
}

func TestTracker_WindowExactParentsPerWindow(t *testing.T) {
	tr := lineage.New()
	win, err := operator.NewTumblingWindow(3, func(w []core.Record) (core.Record, error) {
		return core.Record{Payload: map[string]any{"count": len(w)}}, nil
	})
	require.NoError(t, err)
	op := tr.Wrap("win", win)

	// One batch of 6 → two windows of 3. Each aggregate must point at exactly
	// its own 3 records, NOT all 6 in the batch.
	out, err := op.Process(recs(1, 2, 3, 4, 5, 6))
	require.NoError(t, err)
	require.Len(t, out, 2)

	n0, _ := tr.Get(out[0].ID)
	n1, _ := tr.Get(out[1].ID)
	require.Len(t, n0.Parents, 3)
	require.Len(t, n1.Parents, 3)

	// The two windows partition the inputs — no shared parent.
	first := map[string]bool{}
	for _, p := range n0.Parents {
		first[p] = true
	}
	for _, p := range n1.Parents {
		assert.False(t, first[p], "windows must not share parent %s", p)
	}
	// Roots r1..r3 → window 1; r4..r6 → window 2.
	assert.Equal(t, []string{"r1", "r2", "r3"}, n0.Parents)
	assert.Equal(t, []string{"r4", "r5", "r6"}, n1.Parents)
}

func TestTracker_FlushOutputsCarryParents(t *testing.T) {
	tr := lineage.New()
	win, err := operator.NewTumblingWindow(5, func(w []core.Record) (core.Record, error) {
		return core.Record{Payload: map[string]any{"count": len(w)}}, nil
	})
	require.NoError(t, err)
	op := tr.Wrap("win", win)

	// Only 2 records arrive — the window of 5 never closes during Process.
	out, err := op.Process(recs(1, 2))
	require.NoError(t, err)
	require.Empty(t, out)

	// Flush emits the partial window; its parents must be the 2 buffered records.
	flushed, err := op.(core.Flusher).Flush()
	require.NoError(t, err)
	require.Len(t, flushed, 1)
	assert.Equal(t, 2, flushed[0].Payload["count"])

	node, ok := tr.Get(flushed[0].ID)
	require.True(t, ok)
	assert.Equal(t, []string{"r1", "r2"}, node.Parents)
}

func TestTracker_Ancestors(t *testing.T) {
	tr := lineage.New()
	m1 := tr.Wrap("s1", operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil }))
	m2 := tr.Wrap("s2", operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil }))

	mid, err := m1.Process(recs(1))
	require.NoError(t, err)
	final, err := m2.Process(mid)
	require.NoError(t, err)
	require.Len(t, final, 1)

	anc := tr.Ancestors(final[0].ID)
	// Ancestors: the s1 output + the source root = 2.
	require.Len(t, anc, 2)

	roots := tr.Roots(final[0].ID)
	require.Len(t, roots, 1)
	assert.Equal(t, "source", roots[0].Stage)
}

func TestTracker_Concurrent(t *testing.T) {
	tr := lineage.New()
	ops := make([]core.Operator, 4)
	for i := range ops {
		ops[i] = tr.Wrap("p", operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil }))
	}

	var wg sync.WaitGroup
	for _, op := range ops {
		wg.Add(1)
		go func(o core.Operator) {
			defer wg.Done()
			for range 100 {
				_, err := o.Process(recs(1, 2, 3))
				assert.NoError(t, err)
			}
		}(op)
	}
	wg.Wait()

	// 4 ops × 100 calls × 3 records × 2 nodes (root + output) = 2400.
	assert.Equal(t, 2400, tr.Len())
}

func TestTracker_Export(t *testing.T) {
	tr := lineage.New()
	op := tr.Wrap("m", operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil }))
	_, err := op.Process(recs(1, 2))
	require.NoError(t, err)

	data, err := tr.Export()
	require.NoError(t, err)

	var nodes []lineage.Node
	require.NoError(t, json.Unmarshal(data, &nodes))
	assert.Len(t, nodes, tr.Len())
	assert.Equal(t, 4, len(nodes)) // 2 roots + 2 outputs
}
