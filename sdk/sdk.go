// Package sdk is the single-import fluent facade for embedding the Drift
// streaming engine in a Go service.
//
// Instead of wiring the internal pkg/core, pkg/operator, pkg/source, pkg/sink
// and pkg/pipeline packages by hand, build a pipeline as a method chain:
//
//	var out []sdk.Record
//	err := sdk.New().
//		From(sdk.Slice(in)).
//		Filter(func(r sdk.Record) bool { return r.Payload["v"].(int)%2 == 0 }).
//		Map(func(r sdk.Record) (sdk.Record, error) {
//			r.Payload["v"] = r.Payload["v"].(int) + 1
//			return r, nil
//		}).
//		To(sdk.CollectInto(&out)).
//		Run(ctx)
//
// The facade adds no behaviour of its own: every method maps to an existing
// constructor and execution still goes through pipeline.Pipeline. It must never
// be imported by a pkg/* package.
package sdk

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/gribovan2005/drift/pkg/checkpoint"
	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/lineage"
	"github.com/gribovan2005/drift/pkg/operator"
	"github.com/gribovan2005/drift/pkg/pipeline"
)

// Re-exported core types so callers need only import this package.
type (
	// Record is the fundamental unit of data flowing through a pipeline.
	Record = core.Record
	// Schema describes the shape of Records at a stage.
	Schema = core.Schema
	// Field is a single field descriptor in a Schema.
	Field = core.Field
	// Operator transforms a batch of Records. Implement this for custom stages
	// and add them with Stream.Apply.
	Operator = core.Operator
	// Source produces Records.
	Source = core.Source
	// Sink consumes Records.
	Sink = core.Sink
	// FieldType enumerates the allowed schema field types.
	FieldType = core.FieldType
)

// Re-exported FieldType values.
const (
	String = core.FieldTypeString
	Int    = core.FieldTypeInt
	Float  = core.FieldTypeFloat
	Bool   = core.FieldTypeBool
	Bytes  = core.FieldTypeBytes
	Any    = core.FieldTypeAny
)

// Option configures a Stream's underlying pipeline.
type Option func(*Stream)

// WithLogger sets the structured logger for the pipeline.
func WithLogger(l *slog.Logger) Option {
	return func(s *Stream) { s.popts = append(s.popts, pipeline.WithLogger(l)) }
}

// WithCheckpoint enables periodic state snapshots for stateful operators.
func WithCheckpoint(store checkpoint.Store) Option {
	return func(s *Stream) { s.popts = append(s.popts, pipeline.WithCheckpoint(store)) }
}

// WithLineage enables record-level provenance tracking.
func WithLineage(t *lineage.Tracker) Option {
	return func(s *Stream) { s.popts = append(s.popts, pipeline.WithLineage(t)) }
}

// Stream is a fluent builder for a single pipeline. Build it with New, set a
// source with From, append stages, set a sink with To, then call Run (or Build).
// A Stream produces one pipeline; re-running needs a fresh Stream.
type Stream struct {
	src      core.Source
	stages   []pipeline.Stage
	sink     core.Sink
	popts    []pipeline.Option
	err      error // first builder error; surfaced at Build/Run
	nstage   int   // running count for auto-labels
	branched bool  // true after Branch: the stream is a DAG, no more linear stages
}

// New creates an empty Stream.
func New(opts ...Option) *Stream {
	s := &Stream{}
	for _, o := range opts {
		o(s)
	}
	return s
}

// From sets the source. A second call overwrites the first.
func (s *Stream) From(src Source) *Stream {
	s.src = src
	return s
}

// To sets the sink. A second call overwrites the first.
func (s *Stream) To(sink Sink) *Stream {
	s.sink = sink
	return s
}

// add appends a stage with an auto-generated label (kind-N).
func (s *Stream) add(kind string, op core.Operator) *Stream {
	if s.err != nil {
		return s // short-circuit after a builder error
	}
	if s.branched {
		s.err = fmt.Errorf("drift: cannot append %q after Branch — the stream is a DAG (branches feed the sink directly)", kind)
		return s
	}
	s.nstage++
	s.stages = append(s.stages, pipeline.Stage{
		Label: fmt.Sprintf("%s-%d", kind, s.nstage),
		Op:    op,
	})
	return s
}

// Map appends a one-to-one transform stage.
func (s *Stream) Map(fn func(Record) (Record, error)) *Stream {
	return s.add("map", operator.NewMap(operator.MapFunc(fn)))
}

// Filter appends a stage that keeps only records for which fn returns true.
func (s *Stream) Filter(fn func(Record) bool) *Stream {
	return s.add("filter", operator.NewFilter(operator.PredicateFunc(fn)))
}

// FlatMap appends a one-to-many transform stage.
func (s *Stream) FlatMap(fn func(Record) ([]Record, error)) *Stream {
	return s.add("flatmap", operator.NewFlatMap(operator.FlatMapFunc(fn)))
}

// Tumbling appends a count-based tumbling window that emits one aggregated
// record per `size` input records.
func (s *Stream) Tumbling(size int, agg func(window []Record) (Record, error)) *Stream {
	if s.err != nil {
		return s
	}
	w, err := operator.NewTumblingWindow(size, operator.AggregateFunc(agg))
	if err != nil {
		s.err = fmt.Errorf("tumbling window: %w", err)
		return s
	}
	return s.add("window", w)
}

// Sliding appends a count-based sliding window of `size` records advancing by
// `step` records.
func (s *Stream) Sliding(size, step int, agg func(window []Record) (Record, error)) *Stream {
	if s.err != nil {
		return s
	}
	w, err := operator.NewSlidingWindow(size, step, operator.AggregateFunc(agg))
	if err != nil {
		s.err = fmt.Errorf("sliding window: %w", err)
		return s
	}
	return s.add("window", w)
}

// Deduplicate appends a stage that drops records whose key was seen within the
// given window.
func (s *Stream) Deduplicate(key func(Record) string, window time.Duration) *Stream {
	return s.add("dedup", operator.NewDeduplicate(operator.KeyFunc(key), window))
}

// Apply appends any core.Operator as a stage with an auto-generated label.
// Use this for operators without a dedicated builder method (Join, SessionWindow,
// TopN, SchemaAdapter, custom operators, ...).
func (s *Stream) Apply(op Operator) *Stream {
	return s.add("op", op)
}

// ApplyLabeled appends any core.Operator with an explicit stage label.
func (s *Stream) ApplyLabeled(label string, op Operator) *Stream {
	if s.err != nil {
		return s
	}
	if s.branched {
		s.err = fmt.Errorf("drift: cannot append %q after Branch — the stream is a DAG", label)
		return s
	}
	s.stages = append(s.stages, pipeline.Stage{Label: label, Op: op})
	return s
}

// Build validates the builder and returns the underlying pipeline without
// running it — useful for handing to pkg/web or inspecting Graph/Snapshot.
func (s *Stream) Build() (*pipeline.Pipeline, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.src == nil {
		return nil, fmt.Errorf("drift: no source set (call From)")
	}
	if s.sink == nil {
		return nil, fmt.Errorf("drift: no sink set (call To)")
	}
	stages := s.stages
	if len(stages) == 0 {
		// The executor wires the sink off terminal stages, so a stage-less
		// pipeline would drop every record. Inject an identity passthrough so a
		// pure source→sink stream still works.
		stages = []pipeline.Stage{{
			Label: "passthrough",
			Op:    operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil }),
		}}
	}
	return pipeline.New(s.src, stages, s.sink, s.popts...), nil
}

// Run builds the pipeline and runs it to completion (or until ctx is cancelled).
func (s *Stream) Run(ctx context.Context) error {
	p, err := s.Build()
	if err != nil {
		return err
	}
	return p.Run(ctx)
}
