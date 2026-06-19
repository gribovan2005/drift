package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchema_FieldIndex(t *testing.T) {
	s := Schema{
		ID:      "events",
		Version: 1,
		Fields: []Field{
			{Name: "id", Type: FieldTypeString},
			{Name: "value", Type: FieldTypeFloat},
		},
	}

	assert.Equal(t, 0, s.FieldIndex("id"))
	assert.Equal(t, 1, s.FieldIndex("value"))
	assert.Equal(t, -1, s.FieldIndex("missing"))
}

func TestRecord_PayloadRoundtrip(t *testing.T) {
	r := Record{
		SchemaID:      "events",
		SchemaVersion: 1,
		Payload:       map[string]any{"id": "abc", "value": 3.14},
	}

	assert.Equal(t, "abc", r.Payload["id"])
	assert.InDelta(t, 3.14, r.Payload["value"], 1e-9)
}
