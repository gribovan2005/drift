package job

import (
	"testing"

	"github.com/andrejgribov/drift/pkg/core"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildStageOp_StatelessParallel(t *testing.T) {
	op, err := buildStageOp(StageSpec{
		Label: "f", Op: "filter", Parallelism: 4,
		Params: map[string]any{"field": "v", "cmp": "gte", "value": 5},
	})
	require.NoError(t, err)
	// Behaves like the underlying filter, just sharded.
	out, err := op.Process([]core.Record{
		{Payload: map[string]any{"v": 1}},
		{Payload: map[string]any{"v": 9}},
		{Payload: map[string]any{"v": 7}},
	})
	require.NoError(t, err)
	assert.Len(t, out, 2) // 9 and 7 survive
}

func TestBuildStageOp_KeyedDedupParallel(t *testing.T) {
	op, err := buildStageOp(StageSpec{
		Label: "d", Op: "dedup", Parallelism: 3,
		Params: map[string]any{"key": "id", "window": "1m"},
	})
	require.NoError(t, err)
	out, err := op.Process([]core.Record{
		{Payload: map[string]any{"id": "a"}},
		{Payload: map[string]any{"id": "a"}}, // dup → same shard → dropped
		{Payload: map[string]any{"id": "b"}},
	})
	require.NoError(t, err)
	assert.Len(t, out, 2)
}

func TestBuildStageOp_RejectsGlobalWindow(t *testing.T) {
	_, err := buildStageOp(StageSpec{
		Label: "w", Op: "tumbling", Parallelism: 2,
		Params: map[string]any{"size": 10},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be parallelized")
}

func TestBuildStageOp_RejectsRef(t *testing.T) {
	_, err := buildStageOp(StageSpec{Label: "r", Op: "ref:custom", Parallelism: 2})
	require.Error(t, err)
}
