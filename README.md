# drift

A streaming data processing engine for Go — with live schema evolution and an AI debugger that explains what's happening in plain language.

```
docker run -p 8080:8080 ghcr.io/andrejgribov/drift-demo
```

Open **http://localhost:8080**

---

## Why Drift?

Flink is powerful but heavy: JVM, ZooKeeper, cluster ops, schema change = job restart. Drift is the opposite — a single Go binary, zero external dependencies for the core, and two features Flink doesn't have:

| | Drift | Flink |
|---|---|---|
| **Live Schema Evolution** | Operators adapt mid-stream, zero downtime | Requires job restart |
| **AI Debugger** | Claude explains bottlenecks in plain language | Manual metrics analysis |
| **Deployment** | Single binary | JVM + cluster + ZooKeeper |
| **Startup** | < 50ms | Seconds to minutes |
| **Distributed execution** | Single process (MVP) | Horizontal scaling |
| **Exactly-once** | — | Chandy-Lamport checkpointing |

**Use Drift when** your schema changes frequently, you want a zero-infra streaming layer inside a Go service, or you need to understand what your pipeline is doing without a PhD in distributed systems.

**Use Flink when** you need petabyte-scale, exactly-once guarantees, or distributed fault tolerance.

---

## 5-Minute Quickstart

```bash
# Option 1: Docker (zero install)
docker compose up
# → http://localhost:8080

# Option 2: Build from source
go build ./cmd/demo
./demo
# → http://localhost:8080

# With AI debugging
ANTHROPIC_API_KEY=sk-ant-... ./demo
# → click "Ask Claude" in the UI
```

The demo runs a payment-processing pipeline at ~500 records/sec.  
After **30 seconds**, schema v2 is published live — watch the Schema Evolution panel update with zero downtime.

---

## Core Concepts

```
Source → [Stage 1] → [Stage 2] → … → Sink
```

Each stage is an `Operator` running in its own goroutine, connected by buffered channels. Backpressure is implicit.

### Record

```go
type Record struct {
    SchemaID      string
    SchemaVersion int
    Payload       map[string]any
}
```

### Operator

```go
type Operator interface {
    Process(in []Record) ([]Record, error)
    OnSchemaChange(s Schema)        // called by SchemaRegistry on new version
}
```

### Live Schema Evolution

Publish a new schema version at runtime — subscribed operators receive `OnSchemaChange` immediately and adapt with the next batch. No restart required.

```go
reg := schema.NewRegistry()
reg.Register(v1)

adapter := operator.NewSchemaAdapter(v1, AliasMap{"amount": "value"})
reg.Subscribe("payments", adapter)

// Later, mid-stream:
reg.Register(v2)  // → adapter.OnSchemaChange(v2) called automatically
```

---

## Built-in Operators

| Operator | Description |
|---|---|
| `Map(fn)` | 1-to-1 transform |
| `Filter(pred)` | Keep records matching predicate |
| `FlatMap(fn)` | 1-to-N (or filtering by returning nil) |
| `SchemaAdapter` | Auto-normalise records to current schema (add defaults, apply renames, drop removed fields) |
| `TumblingWindow(size, fn)` | Collect N records → emit one aggregate |
| `SlidingWindow(size, step, fn)` | Overlapping windows; emit every `step` records |

---

## Sources & Sinks

| | In-memory | HTTP | Kafka |
|---|---|---|---|
| **Source** | `source.NewMemory` | `source.NewHTTP` | `source.NewKafka` |
| **Sink** | `sink.NewMemory` | `sink.NewHTTP` | `sink.NewKafka` |

---

## AI Debugger

The AI Debugger collects a snapshot of pipeline metrics (throughput, latency p50/p99, queue depth, error count per stage) and asks Claude to identify bottlenecks and suggest concrete fixes — including a Go config snippet.

```go
dbg := ai.New("", "")  // reads ANTHROPIC_API_KEY from env
explanation, err := dbg.Explain(ctx, p.Snapshot(), p.Graph())
```

Or via the Web UI: click **"Ask Claude"** in the AI Debugger panel.

---

## Web UI

The embedded Web UI updates in real time via Server-Sent Events:

- **Pipeline graph** — live topology with queue depth badges and health colouring
- **Stage cards** — throughput sparklines, p50/p99 latency, error counts
- **Schema Evolution timeline** — version history with field diffs
- **AI Debugger panel** — on-demand Claude analysis

No build step, no npm. The UI is a single embedded HTML/JS/CSS bundle served by the binary.

---

## Project Layout

```
pkg/core       — Record, Schema, Operator, Source, Sink, Flusher interfaces
pkg/schema     — SchemaRegistry (linear versioning, live subscriber notifications)
pkg/operator   — Map, Filter, FlatMap, SchemaAdapter, TumblingWindow, SlidingWindow
pkg/pipeline   — DAG executor with automatic metrics instrumentation
pkg/source     — Memory, Generator, HTTP, Kafka
pkg/sink       — Memory, HTTP, Kafka
pkg/metrics    — StageMetrics (latency ring buffer, throughput window, queue depth)
pkg/ai         — AIDebugger (Claude API integration)
pkg/web        — Embedded Web UI + SSE API server
cmd/demo       — Demo: payment pipeline with live schema evolution
specs/         — Component specs (read before implementing)
skills/        — Claude Code workflow templates
```

---

## Development

```bash
go test ./...                    # full suite (race detector in CI)
go test -bench=. ./tests/bench/  # benchmarks
go run ./cmd/demo                # run demo locally
```

Before adding a component: write a spec in `specs/`, then implement. See `skills/add-operator.md`.

**Throughput baselines (Apple M3):**

| Workload | Records/sec |
|---|---|
| Filter (0 allocs/batch) | ~20M |
| Map | ~6M |
| Map+Filter pipeline | ~2.4M |
| TumblingWindow pipeline | ~7M |

---

## Roadmap

- [ ] Distributed execution (multi-node)
- [ ] Exactly-once semantics (checkpointing)
- [ ] Persistent state (RocksDB)
- [ ] Event time + watermarks
- [ ] Session windows
- [ ] SQL layer

---

## License

MIT
# drift
