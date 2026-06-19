package schema

import (
	"sync"
	"testing"

	"github.com/andrejgribov/drift/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sampleSchema(id string, version int) core.Schema {
	return core.Schema{
		ID:      id,
		Version: version,
		Fields:  []core.Field{{Name: "id", Type: core.FieldTypeString}},
	}
}

func TestRegistry_RegisterAndLatest(t *testing.T) {
	r := NewRegistry()

	require.NoError(t, r.Register(sampleSchema("events", 1)))
	require.NoError(t, r.Register(sampleSchema("events", 2)))

	s, err := r.Latest("events")
	require.NoError(t, err)
	assert.Equal(t, 2, s.Version)
}

func TestRegistry_VersionOutOfOrder(t *testing.T) {
	r := NewRegistry()
	require.NoError(t, r.Register(sampleSchema("events", 1)))

	err := r.Register(sampleSchema("events", 3)) // skip version 2
	assert.ErrorContains(t, err, "expected version 2")
}

func TestRegistry_UnknownSchema(t *testing.T) {
	r := NewRegistry()
	_, err := r.Latest("ghost")
	assert.Error(t, err)
}

func TestRegistry_SubscriberNotified(t *testing.T) {
	r := NewRegistry()

	var mu sync.Mutex
	var received []core.Schema

	op := &captureOperator{onSchemaChange: func(s core.Schema) {
		mu.Lock()
		received = append(received, s)
		mu.Unlock()
	}}

	r.Subscribe("events", op)
	require.NoError(t, r.Register(sampleSchema("events", 1)))
	require.NoError(t, r.Register(sampleSchema("events", 2)))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, received, 2)
	assert.Equal(t, 1, received[0].Version)
	assert.Equal(t, 2, received[1].Version)
}

// captureOperator is a test double that records OnSchemaChange calls.
type captureOperator struct {
	onSchemaChange func(core.Schema)
}

func (c *captureOperator) Process(in []core.Record) ([]core.Record, error) { return in, nil }
func (c *captureOperator) OnSchemaChange(s core.Schema) {
	if c.onSchemaChange != nil {
		c.onSchemaChange(s)
	}
}
