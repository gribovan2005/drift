package operator

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/gribovan2005/drift/pkg/core"
)

// AliasMap maps old field names to their new names after a rename.
type AliasMap map[string]string

// SchemaAdapter sits between a source and downstream operators. It rewrites
// incoming records to match the current schema: adding missing fields with
// their defaults, dropping removed fields, applying renames via AliasMap, and
// coercing each field's value to its declared FieldType (so a widened/changed
// column type evolves live too — e.g. int→float).
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
			newPayload[f.Name] = coerce(v, f.Type)
			continue
		}
		// Check if the field was renamed from an old name.
		for oldName, newName := range aliases {
			if newName == f.Name {
				if v, ok := r.Payload[oldName]; ok {
					newPayload[f.Name] = coerce(v, f.Type)
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

// coerce converts v toward the field's declared type so a type change between schema
// versions takes effect live. Rules: numeric widening is lossless (int→float); float→int
// truncates; →string uses fmt; strings/bools parse when valid. Unparseable values and
// untyped/any/bytes fields pass through unchanged (best-effort — never panics, never
// drops data). nil stays nil.
func coerce(v any, t core.FieldType) any {
	if v == nil {
		return nil
	}
	switch t {
	case core.FieldTypeInt:
		switch n := v.(type) {
		case int64:
			return n
		case int:
			return int64(n)
		case float64:
			return int64(n)
		case bool:
			if n {
				return int64(1)
			}
			return int64(0)
		case string:
			if i, err := strconv.ParseInt(n, 10, 64); err == nil {
				return i
			}
			if f, err := strconv.ParseFloat(n, 64); err == nil {
				return int64(f)
			}
		}
	case core.FieldTypeFloat:
		switch n := v.(type) {
		case float64:
			return n
		case int64:
			return float64(n)
		case int:
			return float64(n)
		case bool:
			if n {
				return 1.0
			}
			return 0.0
		case string:
			if f, err := strconv.ParseFloat(n, 64); err == nil {
				return f
			}
		}
	case core.FieldTypeString:
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprint(v)
	case core.FieldTypeBool:
		switch n := v.(type) {
		case bool:
			return n
		case string:
			if b, err := strconv.ParseBool(n); err == nil {
				return b
			}
		case int64:
			return n != 0
		case int:
			return n != 0
		case float64:
			return n != 0
		}
	}
	return v // untyped / any / bytes / unparseable → unchanged
}
