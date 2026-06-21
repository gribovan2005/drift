package job

import (
	"strings"
	"testing"

	"github.com/andrejgribov/drift/pkg/core"
	"github.com/andrejgribov/drift/pkg/operator"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_LinearJob(t *testing.T) {
	yaml := `
name: linear
source:
  type: generator
  rate: 1ms
  fields:
    id: "tx-${seq}"
    amount: seq
stages:
  - label: keep-big
    op: filter
    field: amount
    gte: 5
  - label: tag
    op: map-set
    field: flagged
    value: true
sink:
  type: memory
`
	b, err := Load([]byte(yaml))
	require.NoError(t, err)
	require.Equal(t, "linear", b.Spec.Name)
	require.Len(t, b.Stages, 2)
	assert.Equal(t, "keep-big", b.Stages[0].Label)
	assert.Equal(t, "tag", b.Stages[1].Label)
	// Linear: first stage's Next is left empty (pipeline resolves it).
	assert.Empty(t, b.Stages[0].Next)
	require.NotNil(t, b.Source)
	require.NotNil(t, b.Sink)
}

func TestLoad_DAGJob(t *testing.T) {
	yaml := `
name: dag
source:
  type: memory
stages:
  - label: root
    op: filter
    field: amount
    gte: 0
    next: [a, b]
  - label: a
    op: map-set
    field: branch
    value: a
  - label: b
    op: map-set
    field: branch
    value: b
sink:
  type: memory
`
	b, err := Load([]byte(yaml))
	require.NoError(t, err)
	require.Len(t, b.Stages, 3)
	assert.Equal(t, []string{"a", "b"}, b.Stages[0].Next)
}

func TestLoad_BuiltinFilter(t *testing.T) {
	yaml := `
name: filt
source:
  type: memory
stages:
  - label: big
    op: filter
    field: amount
    gte: 10
sink:
  type: memory
`
	b, err := Load([]byte(yaml))
	require.NoError(t, err)

	op := b.Stages[0].Op
	out, err := op.Process([]core.Record{
		{Payload: map[string]any{"amount": 5}},
		{Payload: map[string]any{"amount": 10}},
		{Payload: map[string]any{"amount": 20}},
	})
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, 10, out[0].Payload["amount"])
	assert.Equal(t, 20, out[1].Payload["amount"])
}

func TestLoad_RefOperator(t *testing.T) {
	RegisterOp("enrichGo", operator.NewMap(func(r core.Record) (core.Record, error) {
		out := cloneRecord(r)
		out.Payload["enriched"] = true
		return out, nil
	}))

	yaml := `
name: refjob
source:
  type: memory
stages:
  - label: enrich
    op: ref:enrichGo
sink:
  type: memory
`
	b, err := Load([]byte(yaml))
	require.NoError(t, err)

	out, err := b.Stages[0].Op.Process([]core.Record{{Payload: map[string]any{}}})
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, true, out[0].Payload["enriched"])
}

func TestLoad_UnknownOp(t *testing.T) {
	yaml := `
name: bad
source:
  type: memory
stages:
  - label: x
    op: nonsense
sink:
  type: memory
`
	_, err := Load([]byte(yaml))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown op")
}

func TestLoad_DanglingNext(t *testing.T) {
	yaml := `
name: dangling
source:
  type: memory
stages:
  - label: a
    op: filter
    field: amount
    gte: 0
    next: [ghost]
sink:
  type: memory
`
	_, err := Load([]byte(yaml))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestLoad_BadDuration(t *testing.T) {
	yaml := `
name: baddur
source:
  type: memory
stages:
  - label: w
    op: session
    key: id
    gap: "not-a-duration"
sink:
  type: memory
`
	_, err := Load([]byte(yaml))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duration")
}

func TestLoad_MissingName(t *testing.T) {
	_, err := Load([]byte("source:\n  type: memory\n"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name")
}

func TestLoad_DuplicateLabel(t *testing.T) {
	yaml := `
name: dup
source:
  type: memory
stages:
  - label: a
    op: filter
    field: x
    gte: 0
  - label: a
    op: filter
    field: y
    gte: 0
sink:
  type: memory
`
	_, err := Load([]byte(yaml))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}

func TestGraph_Mermaid(t *testing.T) {
	yaml := `
name: g
source:
  type: memory
stages:
  - label: root
    op: filter
    field: amount
    gte: 0
    next: [a, b]
  - label: a
    op: map-set
    field: k
    value: 1
    next: [join]
  - label: b
    op: map-set
    field: k
    value: 2
    next: [join]
  - label: join
    op: map-set
    field: merged
    value: true
sink:
  type: memory
`
	b, err := Load([]byte(yaml))
	require.NoError(t, err)

	out, err := b.Graph("mermaid")
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(out, "graph LR"))
	assert.Contains(t, out, "source[source] --> root[root]")
	assert.Contains(t, out, "root[root] --> a[a]")
	assert.Contains(t, out, "root[root] --> b[b]")
	assert.Contains(t, out, "a[a] --> join[join]")
	assert.Contains(t, out, "b[b] --> join[join]")
	assert.Contains(t, out, "join[join] --> sink[sink]")
}

func TestGraph_JSON(t *testing.T) {
	yaml := `
name: gj
source:
  type: memory
stages:
  - label: only
    op: filter
    field: amount
    gte: 0
sink:
  type: memory
`
	b, err := Load([]byte(yaml))
	require.NoError(t, err)

	out, err := b.Graph("json")
	require.NoError(t, err)
	assert.Contains(t, out, `"name": "gj"`)
	assert.Contains(t, out, `"from": "source"`)
	assert.Contains(t, out, `"to": "sink"`)
}

func TestGraph_UnknownFormat(t *testing.T) {
	b, err := Load([]byte("name: f\nsource:\n  type: memory\nstages:\n  - label: a\n    op: filter\n    field: x\n    gte: 0\nsink:\n  type: memory\n"))
	require.NoError(t, err)
	_, err = b.Graph("svg")
	require.Error(t, err)
}
