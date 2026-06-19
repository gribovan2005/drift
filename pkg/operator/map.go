package operator

import "github.com/andrejgribov/drift/pkg/core"

// MapFunc transforms a single Record into another Record.
type MapFunc func(core.Record) (core.Record, error)

// Map applies fn to each input record.
type Map struct {
	fn     MapFunc
	schema core.Schema
}

// NewMap creates a Map operator with the given transform function.
func NewMap(fn MapFunc) *Map { return &Map{fn: fn} }

func (m *Map) Process(in []core.Record) ([]core.Record, error) {
	out := make([]core.Record, 0, len(in))
	for _, r := range in {
		mapped, err := m.fn(r)
		if err != nil {
			return nil, err
		}
		out = append(out, mapped)
	}
	return out, nil
}

func (m *Map) OnSchemaChange(s core.Schema) { m.schema = s }
