package sdk

import (
	"fmt"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/operator"
	"github.com/gribovan2005/drift/pkg/pipeline"
)

// Branch is a sub-chain builder used inside Stream.Branch. Stages added to it form one
// linear path that runs from the fan-out point to the sink. Like Stream, a builder
// error short-circuits subsequent calls and surfaces at Build/Run.
type Branch struct {
	s      *Stream
	stages []pipeline.Stage
}

// add appends a stage to this branch, wiring the previous branch stage to it.
func (b *Branch) add(kind string, op core.Operator) *Branch {
	if b.s.err != nil {
		return b
	}
	b.s.nstage++
	label := fmt.Sprintf("%s-%d", kind, b.s.nstage)
	if n := len(b.stages); n > 0 {
		b.stages[n-1].Next = []string{label}
	}
	b.stages = append(b.stages, pipeline.Stage{Label: label, Op: op})
	return b
}

// Map appends a one-to-one transform to this branch.
func (b *Branch) Map(fn func(Record) (Record, error)) *Branch {
	return b.add("map", operator.NewMap(operator.MapFunc(fn)))
}

// Filter keeps only records for which fn returns true.
func (b *Branch) Filter(fn func(Record) bool) *Branch {
	return b.add("filter", operator.NewFilter(operator.PredicateFunc(fn)))
}

// FlatMap appends a one-to-many transform to this branch.
func (b *Branch) FlatMap(fn func(Record) ([]Record, error)) *Branch {
	return b.add("flatmap", operator.NewFlatMap(operator.FlatMapFunc(fn)))
}

// Apply appends any core.Operator (vectorized ops, custom operators, ...).
func (b *Branch) Apply(op Operator) *Branch { return b.add("op", op) }

// ApplyLabeled appends any core.Operator with an explicit stage label.
func (b *Branch) ApplyLabeled(label string, op Operator) *Branch {
	if b.s.err != nil {
		return b
	}
	if n := len(b.stages); n > 0 {
		b.stages[n-1].Next = []string{label}
	}
	b.stages = append(b.stages, pipeline.Stage{Label: label, Op: op})
	return b
}

// Branch splits the stream into N independent sub-chains, each running from the current
// tail to the sink (fan-out, then fan-in at the sink). Every function builds one
// branch; at least two are required. After Branch the stream is a DAG — no more linear
// stages may be appended; call To/Run/Build next. The executor copies each record per
// branch (columnar chunks deep-copied, row Payloads shallow-copied), so branches may
// transform independently.
//
//	sdk.New().From(src).Map(parse).
//	    Branch(
//	        func(b *sdk.Branch) { b.Filter(isError).Map(toAlert) },
//	        func(b *sdk.Branch) { b.Map(enrich) },
//	    ).
//	    To(sink).Run(ctx)
//	// both branches' outputs merge into sink
func (s *Stream) Branch(fns ...func(*Branch)) *Stream {
	if s.err != nil {
		return s
	}
	if s.branched {
		s.err = fmt.Errorf("drift: Branch already called — a stream supports one fan-out")
		return s
	}
	if len(fns) < 2 {
		s.err = fmt.Errorf("drift: Branch needs ≥2 branches, got %d", len(fns))
		return s
	}

	// Once the graph has any explicit Next the executor stops auto-linking by slice
	// order, so the linear prefix must be wired here too.
	prefixLen := len(s.stages)
	if prefixLen == 0 {
		// No prefix: inject a passthrough anchor so the graph has explicit Next (else
		// the executor would auto-link the branch heads into a chain). The source fans
		// out to the branches through it.
		s.nstage++
		s.stages = append(s.stages, pipeline.Stage{
			Label: fmt.Sprintf("split-%d", s.nstage),
			Op:    operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil }),
		})
		prefixLen = 1
	}
	for i := 0; i+1 < prefixLen; i++ {
		s.stages[i].Next = []string{s.stages[i+1].Label}
	}

	var firstLabels []string
	var branchStages []pipeline.Stage
	for _, fn := range fns {
		b := &Branch{s: s}
		fn(b)
		if s.err != nil {
			return s
		}
		if len(b.stages) == 0 {
			// An empty branch tees the input straight to the sink.
			s.nstage++
			b.stages = []pipeline.Stage{{
				Label: fmt.Sprintf("branch-%d", s.nstage),
				Op:    operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil }),
			}}
		}
		firstLabels = append(firstLabels, b.stages[0].Label)
		branchStages = append(branchStages, b.stages...)
	}

	if prefixLen > 0 {
		// Fan out the tail to every branch head. With no prefix, the branch heads
		// become roots and the source fans out to them.
		s.stages[prefixLen-1].Next = firstLabels
	}
	s.stages = append(s.stages, branchStages...)
	s.branched = true
	return s
}
