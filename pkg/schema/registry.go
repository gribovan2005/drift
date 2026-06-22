package schema

import (
	"fmt"
	"sync"

	"github.com/gribovan2005/drift/pkg/core"
)

// Registry stores versioned schemas and notifies subscribers when a new
// version is published.
type Registry struct {
	mu          sync.RWMutex
	schemas     map[string][]core.Schema  // schemaID → ordered versions (index == version-1)
	subscribers map[string][]core.Operator // schemaID → operators to notify
}

// NewRegistry creates an empty Schema Registry.
func NewRegistry() *Registry {
	return &Registry{
		schemas:     make(map[string][]core.Schema),
		subscribers: make(map[string][]core.Operator),
	}
}

// Register publishes a new schema version. The Version field must be exactly
// len(existing versions)+1 to enforce linear history.
func (r *Registry) Register(s core.Schema) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	versions := r.schemas[s.ID]
	expected := len(versions) + 1
	if s.Version != expected {
		return fmt.Errorf("schema %q: expected version %d, got %d", s.ID, expected, s.Version)
	}

	r.schemas[s.ID] = append(versions, s)

	// Notify subscribers outside the lock to avoid deadlocks if an operator
	// calls back into the registry during OnSchemaChange.
	subs := make([]core.Operator, len(r.subscribers[s.ID]))
	copy(subs, r.subscribers[s.ID])

	r.mu.Unlock()
	for _, op := range subs {
		op.OnSchemaChange(s)
	}
	r.mu.Lock() // re-acquire for deferred unlock

	return nil
}

// Latest returns the most recent schema for the given ID, or an error if
// the ID is unknown.
func (r *Registry) Latest(id string) (core.Schema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions, ok := r.schemas[id]
	if !ok || len(versions) == 0 {
		return core.Schema{}, fmt.Errorf("schema %q not found", id)
	}
	return versions[len(versions)-1], nil
}

// Version returns the exact schema version for the given ID and version number.
func (r *Registry) Version(id string, version int) (core.Schema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions, ok := r.schemas[id]
	if !ok {
		return core.Schema{}, fmt.Errorf("schema %q not found", id)
	}
	if version < 1 || version > len(versions) {
		return core.Schema{}, fmt.Errorf("schema %q: version %d out of range [1, %d]", id, version, len(versions))
	}
	return versions[version-1], nil
}

// Subscribe registers an operator to receive OnSchemaChange calls whenever a
// new version of the given schema ID is published.
func (r *Registry) Subscribe(schemaID string, op core.Operator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subscribers[schemaID] = append(r.subscribers[schemaID], op)
}

// AllVersions returns a copy of every schema version for the given ID,
// ordered from oldest (v1) to newest. Returns nil if ID is unknown.
func (r *Registry) AllVersions(id string) []core.Schema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	versions := r.schemas[id]
	result := make([]core.Schema, len(versions))
	copy(result, versions)
	return result
}

// SchemaIDs returns all registered schema IDs in arbitrary order.
func (r *Registry) SchemaIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.schemas))
	for id := range r.schemas {
		ids = append(ids, id)
	}
	return ids
}
