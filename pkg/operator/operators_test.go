package operator

import (
	"errors"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Map ────────────────────────────────────────────────────────────────────

func TestMap_TransformsPayload(t *testing.T) {
	op := NewMap(func(r core.Record) (core.Record, error) {
		r.Payload["x"] = r.Payload["x"].(int) * 2
		return r, nil
	})

	out, err := op.Process([]core.Record{
		{Payload: map[string]any{"x": 3}},
		{Payload: map[string]any{"x": 5}},
	})
	require.NoError(t, err)
	assert.Equal(t, 6, out[0].Payload["x"])
	assert.Equal(t, 10, out[1].Payload["x"])
}

func TestMap_PropagatesError(t *testing.T) {
	boom := errors.New("boom")
	op := NewMap(func(r core.Record) (core.Record, error) {
		return core.Record{}, boom
	})
	_, err := op.Process([]core.Record{{Payload: map[string]any{}}})
	assert.ErrorIs(t, err, boom)
}

func TestMap_EmptyInput(t *testing.T) {
	op := NewMap(func(r core.Record) (core.Record, error) { return r, nil })
	out, err := op.Process(nil)
	require.NoError(t, err)
	assert.Empty(t, out)
}

// ── Filter ─────────────────────────────────────────────────────────────────

func TestFilter_PassesMatchingRecords(t *testing.T) {
	op := NewFilter(func(r core.Record) bool {
		return r.Payload["keep"].(bool)
	})

	in := []core.Record{
		{Payload: map[string]any{"keep": true}},
		{Payload: map[string]any{"keep": false}},
		{Payload: map[string]any{"keep": true}},
	}
	out, err := op.Process(in)
	require.NoError(t, err)
	assert.Len(t, out, 2)
	assert.True(t, out[0].Payload["keep"].(bool))
}

func TestFilter_Idempotent(t *testing.T) {
	// Applying the same filter twice must produce the same result as once.
	pred := func(r core.Record) bool { return r.Payload["v"].(int) > 2 }
	op := NewFilter(pred)

	in := []core.Record{
		{Payload: map[string]any{"v": 1}},
		{Payload: map[string]any{"v": 3}},
		{Payload: map[string]any{"v": 5}},
	}

	first, _ := op.Process(in)
	second, _ := op.Process(first)
	assert.Equal(t, first, second)
}

// ── FlatMap ────────────────────────────────────────────────────────────────

func TestFlatMap_ExpandsRecords(t *testing.T) {
	op := NewFlatMap(func(r core.Record) ([]core.Record, error) {
		n := r.Payload["n"].(int)
		out := make([]core.Record, n)
		for i := range out {
			out[i] = core.Record{Payload: map[string]any{"i": i}}
		}
		return out, nil
	})

	out, err := op.Process([]core.Record{
		{Payload: map[string]any{"n": 3}},
	})
	require.NoError(t, err)
	assert.Len(t, out, 3)
}

func TestFlatMap_CanFilterByReturningEmpty(t *testing.T) {
	op := NewFlatMap(func(r core.Record) ([]core.Record, error) {
		if r.Payload["drop"].(bool) {
			return nil, nil
		}
		return []core.Record{r}, nil
	})

	in := []core.Record{
		{Payload: map[string]any{"drop": true}},
		{Payload: map[string]any{"drop": false}},
	}
	out, err := op.Process(in)
	require.NoError(t, err)
	assert.Len(t, out, 1)
}

// ── SchemaAdapter ──────────────────────────────────────────────────────────

var v1Schema = core.Schema{
	ID: "events", Version: 1,
	Fields: []core.Field{
		{Name: "id", Type: core.FieldTypeString},
		{Name: "value", Type: core.FieldTypeFloat},
	},
}

var v2Schema = core.Schema{
	ID: "events", Version: 2,
	Fields: []core.Field{
		{Name: "id", Type: core.FieldTypeString},
		{Name: "score", Type: core.FieldTypeFloat, Default: 0.0}, // renamed from "value"
		{Name: "tag", Type: core.FieldTypeString, Default: ""},   // new field
	},
}

func TestSchemaAdapter_AddsMissingFields(t *testing.T) {
	adapter := NewSchemaAdapter(v2Schema, nil)

	// Old record that has no "tag" or "score".
	in := []core.Record{{
		SchemaID: "events", SchemaVersion: 1,
		Payload: map[string]any{"id": "x", "value": 1.5},
	}}

	out, err := adapter.Process(in)
	require.NoError(t, err)
	assert.Equal(t, "", out[0].Payload["tag"])
	assert.Equal(t, 2, out[0].SchemaVersion)
}

func TestSchemaAdapter_AppliesRename(t *testing.T) {
	aliases := AliasMap{"value": "score"}
	adapter := NewSchemaAdapter(v2Schema, aliases)

	in := []core.Record{{
		Payload: map[string]any{"id": "y", "value": 9.9},
	}}

	out, err := adapter.Process(in)
	require.NoError(t, err)
	assert.Equal(t, 9.9, out[0].Payload["score"])
	_, hasOld := out[0].Payload["value"]
	assert.False(t, hasOld, "old field name must not appear in output")
}

func TestSchemaAdapter_DropsRemovedFields(t *testing.T) {
	adapter := NewSchemaAdapter(v2Schema, AliasMap{"value": "score"})

	// Record has an extra field "legacy" not in v2Schema.
	in := []core.Record{{
		Payload: map[string]any{"id": "z", "value": 1.0, "legacy": "old"},
	}}
	out, err := adapter.Process(in)
	require.NoError(t, err)
	_, hasLegacy := out[0].Payload["legacy"]
	assert.False(t, hasLegacy)
}

func TestSchemaAdapter_LiveEvolution(t *testing.T) {
	adapter := NewSchemaAdapter(v1Schema, nil)

	// Records under v1 pass through unchanged (relative to v1 fields).
	r1 := core.Record{Payload: map[string]any{"id": "a", "value": 1.0}}
	out1, err := adapter.Process([]core.Record{r1})
	require.NoError(t, err)
	assert.Equal(t, 1, out1[0].SchemaVersion)

	// Schema evolves — no restart.
	adapter.OnSchemaChange(v2Schema)

	r2 := core.Record{Payload: map[string]any{"id": "b", "value": 2.0}}
	out2, err := adapter.Process([]core.Record{r2})
	require.NoError(t, err)
	assert.Equal(t, 2, out2[0].SchemaVersion)
	assert.Equal(t, "", out2[0].Payload["tag"], "new field gets default")
}
