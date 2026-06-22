---
component: sdk
status: stable
package: sdk (subpackage `drift/sdk`)
tested: true
---

# SDK (embeddable Go library)

The subpackage `github.com/gribovan2005/drift/sdk` is a **single-import fluent
facade** over the internal `pkg/*` packages. It exists so other Go services can
embed Drift without learning the `core`/`operator`/`source`/`sink`/`pipeline`
layout:

```go
import "github.com/gribovan2005/drift/sdk"

var out []sdk.Record
err := sdk.New().
    From(sdk.Slice(in)).
    Filter(func(r sdk.Record) bool { return r.Payload["v"].(int)%2 == 0 }).
    Map(func(r sdk.Record) (sdk.Record, error) {
        r.Payload["v"] = r.Payload["v"].(int) + 1
        return r, nil
    }).
    To(sdk.CollectInto(&out)).
    Run(ctx)
```

**Design rule:** the facade adds NO behaviour — it only re-exports types and
composes existing constructors into a builder. All execution still goes through
`pipeline.Pipeline`. The facade must never be imported by any `pkg/*` package
(it sits above them; importing it would create a cycle). `pkg/core`'s import rule
is unaffected.

---

## Type re-exports

Aliases (not new types — assignable to/from the underlying types):

```go
type Record   = core.Record
type Schema    = core.Schema
type Field     = core.Field
type Operator  = core.Operator
type Source    = core.Source
type Sink      = core.Sink
type FieldType = core.FieldType

const (
    String = core.FieldTypeString
    Int    = core.FieldTypeInt
    Float  = core.FieldTypeFloat
    Bool   = core.FieldTypeBool
    Bytes  = core.FieldTypeBytes
    Any    = core.FieldTypeAny
)
```

---

## Stream builder

```go
type Stream struct { ... }            // opaque; build with New()

func New(opts ...Option) *Stream
```

Builder options (wrap `pipeline.Option`):

```go
func WithLogger(l *slog.Logger) Option
func WithCheckpoint(s checkpoint.Store) Option
func WithLineage(t *lineage.Tracker) Option
```

### Source / sink

```go
func (s *Stream) From(src Source) *Stream   // exactly one; second call overwrites
func (s *Stream) To(sink Sink) *Stream       // exactly one; second call overwrites
```

### Stage methods (each appends one stage, returns the Stream)

```go
func (s *Stream) Map(fn func(Record) (Record, error)) *Stream
func (s *Stream) Filter(fn func(Record) bool) *Stream
func (s *Stream) FlatMap(fn func(Record) ([]Record, error)) *Stream
func (s *Stream) Tumbling(size int, agg func(window []Record) (Record, error)) *Stream
func (s *Stream) Sliding(size, step int, agg func(window []Record) (Record, error)) *Stream
func (s *Stream) Deduplicate(key func(Record) string, window time.Duration) *Stream
func (s *Stream) Apply(op Operator) *Stream            // escape hatch: any core.Operator
func (s *Stream) ApplyLabeled(label string, op Operator) *Stream
```

- **Auto-labels:** stages get labels `map-1`, `filter-2`, `window-3`, ... in chain
  order (kind + global 1-based index). `ApplyLabeled` overrides. Labels are what
  the monitor/graph display.
- **Deferred errors:** constructors that can fail (`Tumbling`, `Sliding`) capture
  the error on the Stream; the first such error is returned by `Run`/`Build` and
  short-circuits later stage methods (they become no-ops).

### Terminal methods

```go
func (s *Stream) Build() (*pipeline.Pipeline, error)  // for monitoring/web wiring
func (s *Stream) Run(ctx context.Context) error        // Build + Pipeline.Run
```

`Build`/`Run` error if: a builder step failed, `From` was never called, or `To`
was never called. `Build` lets callers hand the pipeline to `pkg/web` or inspect
`Graph()`/`Snapshot()`.

**Stage-less passthrough:** the executor wires the sink off terminal *stages*, so a
pipeline with zero stages would drop every record. When no stage method was
called, `Build` injects a single identity stage labelled `passthrough` so a pure
`From(...).To(...)` stream still copies source → sink. This is an identity no-op,
consistent with the pure-composition rule.

---

## Source helpers

```go
func Slice(records []Record) Source                       // = source.NewMemory
func Generate(fn func(seq int) Record, every time.Duration) Source  // = source.NewGenerator
func HTTPSource(addr string) Source                        // = source.NewHTTP
func KafkaSource(cfg source.KafkaConfig) Source            // = source.NewKafka
```

## Sink helpers

```go
func Collect() *Collector                 // in-memory; Collector.Records() []Record
func CollectInto(dst *[]Record) Sink       // appends arrivals into *dst
func Discard() Sink                        // drops everything (load tests)
func ToFile(path string) Sink              // = sink.NewFile (NDJSON)
func HTTPSink(url string) Sink             // = sink.NewHTTP
func KafkaSink(cfg sink.KafkaConfig) (Sink, error)  // = sink.NewKafka

type Collector struct { ... }              // implements Sink
func (c *Collector) Records() []Record     // safe after Run returns
```

- `CollectInto`/`Collector` write from a single goroutine (the sink's Write loop),
  so no locking is needed for the slice; `Records()` returns a copy.
- `Discard` is the facade's load-test sink (drains the channel, keeps nothing).

---

## Invariants

1. The facade is **pure composition** — every method maps to an existing
   constructor; no new execution logic.
2. A `Stream` builds **one** `Pipeline`; `Run` is `Build` + `Pipeline.Run`. Re-running
   requires a fresh `Stream` (same single-shot rule as `pipeline.Pipeline`).
3. `From`/`To` are mandatory; missing either is a `Build`/`Run` error, not a panic.
4. The first builder error wins and is surfaced at `Build`/`Run`; later stage
   methods after an error are no-ops (safe to keep chaining).
5. No `pkg/*` package imports the sdk facade (would cycle). The facade may import
   any `pkg/*`.

---

## Required tests (package `sdk_test`, no mocks, `-race` green)

- `Map_Filter_Collect` — full chain via `Slice` → `Filter` → `Map` → `CollectInto`.
- `FlatMap` — one-to-many expansion.
- `Tumbling` / `Sliding` — windowed aggregation through the facade.
- `Deduplicate` — dedup semantics preserved.
- `Apply_EscapeHatch` — a `core.Operator` (e.g. a Join or SchemaAdapter) runs via `Apply`.
- `AutoLabels` — `Build().Graph()` labels are `map-1`/`filter-2`/... in order.
- `MissingSource_Errors` / `MissingSink_Errors` — `Run` returns an error, no panic.
- `BuilderError_Propagates` — bad `Tumbling(0,...)` surfaces at `Run`.
- `Collector_Records` — `Collect()` returns all records after `Run`.
- `Build_ForMonitoring` — `Build()` returns a usable `*pipeline.Pipeline` (Graph non-empty).

---

## See also

- [[Core Abstractions]] — the underlying Record/Operator/Source/Sink contracts
- [[Operators]] — what each stage method maps to
- [[Sources & Sinks]] — what the source/sink helpers map to
- [[CLI & Jobs]] — the YAML/CLI surface (the SDK is the programmatic peer)
- [[Distribution]] — module path (`go get github.com/gribovan2005/drift`)
