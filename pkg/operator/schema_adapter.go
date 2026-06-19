package operator

import (
	"sync"

	"github.com/andrejgribov/drift/pkg/core"
)

// AliasMap maps old field names to their new names after a rename.
type AliasMap map[string]string

// SchemaAdapter sits between a source and downstream operators. It rewrites
// incoming records to match the current schema: adding missing fields with
// their defaults, dropping removed fields, and applying renames via AliasMap.
//
// Schema changes take effect on the next call to Process after OnSchemaChange.
type SchemaAdapter struct {
	mu       sync.RWMutex
	current  core.Schema
	aliases  AliasMap
}

// NewSchemaAdapter creates a SchemaAdapter with an initial schema.
func NewSchemaAdapter(initial core.Schema, aliases AliasMap) *SchemaAdapter {
	if aliases == nil {
		aliases = AliasMap{}
	}
	return &SchemaAdapter{current: initial, aliases: aliases}
}

func (a *SchemaAdapter) OnSchemaChange(s core.Schema) {
	a.mu.Lock()
	a.current = s
	a.mu.Unlock()
}

func (a *SchemaAdapter) Process(in []core.Record) ([]core.Record, error) {
	a.mu.RLock()
	schema := a.current
	aliases := a.aliases
	a.mu.RUnlock()

	out := make([]core.Record, 0, len(in))
	for _, r := range in {
		out = append(out, a.adapt(r, schema, aliases))
	}
	return out, nil
}

func (a *SchemaAdapter) adapt(r core.Record, s core.Schema, aliases AliasMap) core.Record {
	newPayload := make(map[string]any, len(s.Fields))

	for _, f := range s.Fields {
		// Check if the record has the field under its current name.
		if v, ok := r.Payload[f.Name]; ok {
			newPayload[f.Name] = v
			continue
		}
		// Check if the field was renamed from an old name.
		for oldName, newName := range aliases {
			if newName == f.Name {
				if v, ok := r.Payload[oldName]; ok {
					newPayload[f.Name] = v
					goto next
				}
			}
		}
		// Field is new — use its default value (nil if not set).
		newPayload[f.Name] = f.Default
	next:
	}

	return core.Record{
		SchemaID:      s.ID,
		SchemaVersion: s.Version,
		Payload:       newPayload,
	}
}
