---
component: cli-jobs
status: implemented
package: pkg/job
file: pkg/job/loader.go
tested: true
example: examples/fraud.yaml
---

# CLI & Jobs

Declarative pipelines without recompiling. A **job** is a YAML file describing a
source, a DAG of operator stages, and a sink. The CLI loads it, builds a
`pipeline.Pipeline`, and runs it.

**Hybrid model:** common operators are configured by data (built-ins); arbitrary
Go logic is referenced by name via `ref:<name>` from an in-process registry the
host program populates before calling the loader.

---

## Job spec (YAML)

```yaml
name: fraud
profile: sidecar         # optional: sidecar | dedicated — tunes batch/buffer/linger ([[Resource Profiles]])
source:
  type: generator        # generator | memory | http | ref:<name>
  rate: 1ms
  fields:                # field templates (see below)
    id: seq              # numeric sequence value (0,1,2,…)
    ref: "tx-${seq}"     # ${seq} substituted into the string
    amount: rand:int:1:500   # pseudo-random int in [min,max]
    score: rand:float:0:1    # pseudo-random float in [min,max)
    region: choice:eu|us|apac  # cycles options by sequence
    note: "eu-west"      # anything else: verbatim
stages:
  - label: filter-small
    op: filter           # built-in
    field: amount
    cmp: gte             # gte | lte | eq
    value: 10
    next: [enrich]
  - label: enrich
    op: ref:enrichGo     # resolved from the registry (Go closure)
sink:
  type: memory           # memory | http | ref:<name>
```

- `stages[].next` wires the DAG (same semantics as `pipeline.Stage.Next`). When
  omitted, stages run in declaration order (linear).
- `name` is used for logs and lineage.
- `profile` (optional) applies a resource preset's local knobs (batch/buffer/linger)
  via `sdk.ProfileByName`; unknown values fail validation. Process-global knobs are
  not applied from a job. See [[Resource Profiles]].

---

## Built-in operators

| `op` | Params | Maps to |
|---|---|---|
| `filter` | `field` + `cmp` (`gte`/`lte`/`eq`) + `value`; legacy `gte`/`lte`/`eq` keys still work | `operator.NewFilter` |
| `map-set` | `field`, `value` | `operator.NewMap` (sets a constant field) |
| `map-rename` | `from`, `to` | `operator.NewMap` (renames a field) |
| `dedup` | `key` (field), `window` (duration) | `operator.NewDeduplicate` |
| `tumbling` | `size` (int), `agg` | `operator.NewTumblingWindow` |
| `timestamp` | `field` | `operator.NewTimestampAssigner` |
| `eventwindow` | `size` (dur), `lateness` (dur), `agg` | `operator.NewEventTimeWindow` |
| `session` | `key` (field), `gap` (dur), `agg` | `operator.NewSessionWindow` |

**Aggregations (`agg`):** `count` (emits `{count: n}`) or `sum:<field>` (emits
`{sum: Σfield}`). Durations use Go syntax (`5s`, `100ms`).

**`timestamp` field values:** a `time.Time`, an RFC3339(Nano) string, or a number
read as Unix seconds; anything else yields the zero time.

**`filter` comparison:** `gte`/`lte` coerce the field to a number (records whose
field is non-numeric are dropped); `eq` compares for equality against the literal.

**`ref:<name>`** resolves an operator the host registered with
`job.RegisterOp(name, op)`. This is the escape hatch for logic YAML can't express.

---

## Sources & sinks

| `type` | Params | Maps to |
|---|---|---|
| `generator` | `rate` (dur), `fields` (template) | `source.NewGenerator` |
| `memory` | (records via `ref:` only) | `source.NewMemory` |
| `http` | `addr` | `source.NewHTTP` |
| `memory` (sink) | — | `sink.NewMemory` |
| `http` (sink) | `url` | `sink.NewHTTP` |
| `ref:<name>` | — | host-registered via `job.RegisterSource` / `job.RegisterSink` |

---

## Registry API

```go
// Host registers code-defined components before loading a job.
func RegisterOp(name string, op core.Operator)
func RegisterSource(name string, s core.Source)
func RegisterSink(name string, s core.Sink)

// Load parses YAML and builds a runnable pipeline.
func Load(data []byte) (*Built, error)
type Built struct {
    Spec    Spec
    Source  core.Source
    Stages  []pipeline.Stage
    Sink    core.Sink
}
func (b *Built) Pipeline() *pipeline.Pipeline
```

`Load` validates: unique stage labels, every `next` target exists, known `op`
type, required params present and well-typed, resolvable `ref:` names.

---

## CLI

```
drift run <job.yaml> [--ui] [--lineage <file>]  # build + run; --ui serves the dashboard; --lineage writes the record-level provenance graph as JSON on exit
drift validate <job.yaml>        # parse + validate, print OK or the first error
drift graph <job.yaml> [--format mermaid|dot|json]   # print the DAG (lineage, static)
drift list                       # list registered ops / sources / sinks
drift serve --jobs-dir <dir> [--addr :8080]   # control plane: build/save/run jobs from the web UI
drift version                    # print version / commit / build date
```

`drift version` prints build metadata. The `version`/`commit`/`date` vars in
`cmd/drift/main.go` default to `dev`/`none`/`unknown` and are overwritten at
release time via `-ldflags -X main.version=…` (see [[Distribution]]).

`drift graph` is the **static lineage** view (stage-level). Record-level
provenance is tracked separately — see [[Lineage]] (`drift run --lineage`).

`drift serve` starts the [[Control Plane]] + [[Web UI & Builder]]: a runner over a
folder of YAML jobs, with a visual DAG builder. Honours `DRIFT_AUTH_TOKEN`.

## Programmatic helpers

- `job.Marshal(spec) ([]byte, error)` — Spec → YAML (round-trips with `Parse`/`Load`).
- `job.Catalog() Palette` — declarative block catalog (every source/op/sink + its
  param schema); single source of truth for the builder palette. See [[Web UI & Builder]].

---

## Required tests

| Test | Proves |
|---|---|
| `TestLoad_LinearJob` | YAML → runnable linear pipeline |
| `TestLoad_DAGJob` | `next` wiring builds the right graph |
| `TestLoad_BuiltinFilter` | `filter` op applies field/gte |
| `TestLoad_RefOperator` | `ref:` resolves a registered operator |
| `TestLoad_UnknownOp` | unknown `op` → error |
| `TestLoad_DanglingNext` | `next` to missing label → error |
| `TestLoad_BadDuration` | malformed duration → error |
| `TestGraph_Mermaid` | graph export matches expected DAG |
| `TestMarshal_RoundTrip` | Spec → YAML → Spec is stable (incl. inline params, `next`) |
| `TestCatalog_CoversAllOps` | catalog ⇔ loader switches stay in sync |
| `TestCatalog_DefaultsLoad` | each block's required+default params load |

---

## See also

- [[Operators]]
- [[Sources & Sinks]]
- [[Control Plane]] — `drift serve`, runner + job store
- [[Web UI & Builder]] — visual builder over the catalog
- [[Overview#Execution model]]
