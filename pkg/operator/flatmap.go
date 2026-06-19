package operator

import "github.com/andrejgribov/drift/pkg/core"

// FlatMapFunc transforms one Record into zero or more Records.
type FlatMapFunc func(core.Record) ([]core.Record, error)

// FlatMap applies fn to each record and flattens the results.
type FlatMap struct {
	fn     FlatMapFunc
	schema core.Schema
}

// NewFlatMap creates a FlatMap operator with the given function.
func NewFlatMap(fn FlatMapFunc) *FlatMap { return &FlatMap{fn: fn} }

func (fm *FlatMap) Process(in []core.Record) ([]core.Record, error) {
	out := make([]core.Record, 0, len(in))
	for _, r := range in {
		expanded, err := fm.fn(r)
		if err != nil {
			return nil, err
		}
		out = append(out, expanded...)
	}
	return out, nil
}

func (fm *FlatMap) OnSchemaChange(s core.Schema) { fm.schema = s }
