package operator

import "github.com/andrejgribov/drift/pkg/core"

// PredicateFunc returns true if the record should pass through.
type PredicateFunc func(core.Record) bool

// Filter passes only records for which pred returns true.
type Filter struct {
	pred   PredicateFunc
	schema core.Schema
}

// NewFilter creates a Filter operator with the given predicate.
func NewFilter(pred PredicateFunc) *Filter { return &Filter{pred: pred} }

func (f *Filter) Process(in []core.Record) ([]core.Record, error) {
	out := in[:0] // reuse backing array
	for _, r := range in {
		if f.pred(r) {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *Filter) OnSchemaChange(s core.Schema) { f.schema = s }
