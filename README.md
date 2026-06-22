# drift

A streaming data processing engine for Go — with live schema evolution and an AI debugger that explains what's happening in plain language.

## Install

```bash
# Homebrew (macOS / Linux)
brew install gribovan2005/drift/drift
drift version

# or run the demo with Docker
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

## Use as a library

Embed Drift directly in a Go service — one import, a fluent builder:

```bash
go get github.com/gribovan2005/drift/sdk
```

```go
import "github.com/gribovan2005/drift/sdk"

out := sdk.Collect()
err := sdk.New().
    From(sdk.Slice(in)).                                       // or Kafka/HTTP/Generate
    Filter(func(r sdk.Record) bool { return r.Payload["v"].(int)%2 == 0 }).
    Map(func(r sdk.Record) (sdk.Record, error) {
        r.Payload["v"] = r.Payload["v"].(int) + 1
        return r, nil
    }).
    Tumbling(64, aggregate).                                   // windows, dedup, joins…
    To(out).                                                   // or Kafka/File/HTTP
    Run(ctx)
```

The `sdk` package is a thin facade over `pkg/*`; use `Apply(op)` for any operator
without a dedicated method, and `Build()` to get the raw `*pipeline.Pipeline` for
monitoring. See [`drift/Specs/SDK.md`](drift/Specs/SDK.md).

**Observability:** expose per-stage metrics to your existing Prometheus — no agent,
no cluster:

```go
p, _ := sdk.New().From(src).Map(fn).To(sink).Build()
http.Handle("/metrics", sdk.PrometheusHandler(p))   // drift_stage_processed_total, …
go p.Run(ctx)
```

**Runnable example** — a real-time payment-analytics service in one file (live
materialized view over HTTP, live schema evolution, Prometheus metrics):

```bash
go run ./cmd/sdkdemo
curl localhost:8090/stats     # live window aggregates, no database
curl localhost:8090/metrics   # Prometheus scrape
# at t+15s the schema evolves live — watch "schema_has_risk_score" flip to true
```

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

## Web UI & visual builder

The embedded Web UI has two views, both served by the binary (no build step, no npm —
a single embedded HTML/JS/CSS bundle).

**Monitor** updates in real time via Server-Sent Events:

- **Pipeline graph** — live topology with queue depth badges and health colouring
- **Stage cards** — throughput sparklines, p50/p99 latency, error counts (click a card
  for an advanced per-stage metrics drawer with pipeline totals + uptime)
- **Schema Evolution timeline** — version history with field diffs
- **AI Debugger panel** — on-demand Claude analysis
- Auto-reconnecting SSE, idle/empty states

**Builder** is a drag-and-drop DAG editor backed by a control plane:

```bash
go run ./cmd/drift serve --jobs-dir ./jobs   # → http://localhost:8080 (Builder tab)
```

- Drag source / operator / sink blocks onto the canvas, wire operators, configure params
- **Save → YAML** in the jobs folder, and load a YAML back into the canvas (round-trip)
- **Run / Stop** pipelines from the UI; the Monitor follows the running pipeline live
- Built on `job.Catalog()` so the palette always matches the engine's built-ins
- Set `DRIFT_AUTH_TOKEN` to require a bearer token on the API

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
pkg/lineage    — Record-level provenance tracker
pkg/wal        — Write-ahead log + exactly-once coordinator
pkg/job        — Declarative YAML jobs + operator/source/sink catalog
pkg/runner     — Control plane: job store (YAML folder) + pipeline runner
pkg/web        — Embedded Web UI (monitor + builder) + SSE/control-plane API
cmd/demo       — Demo: payment pipeline with live schema evolution
cmd/drift      — CLI: run / validate / graph / list / serve
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

**Throughput baselines (Apple M3, raw operator `Process`):**

| Workload | Records/sec |
|---|---|
| Filter (0 allocs/batch) | ~20M |
| Map | ~6M |
| Map+Filter pipeline | ~2.4M |
| TumblingWindow pipeline | ~7M |

**Nexmark vs Flink:** Drift implements the **full Nexmark suite (all 23 queries,
q0–q22)** — stateless, windowed, group-by, top-N, joins, and a file sink. A
**same-machine** run (Flink 1.18 in Docker on the same laptop) shows the two are
**comparable per core** on stateless queries (Drift ~1.0–1.5×); Flink scales
better across cores on one box for a single query. Drift's edge is operational
(single binary, no JVM/cluster/shuffle), not raw throughput. Full methodology,
same-iron tables, and the per-query results: [BENCHMARKS.md](BENCHMARKS.md).

---

## Roadmap

Single-process production-grade path (Path A) — **complete**:

- [x] True DAG executor (fan-out / fan-in)
- [x] Event time + watermarks
- [x] Session windows
- [x] Persistent state backend (BadgerDB)
- [x] Exactly-once semantics (WAL + idempotent sink)
- [x] Record-level lineage
- [x] CLI + declarative YAML jobs
- [x] Visual builder + control plane (`drift serve`)

Beyond Path A:

- [ ] Distributed execution (multi-node)
- [ ] SQL layer
- [ ] More connectors (CDC, object storage)

---

## Releasing (maintainer)

Releases are automated with [GoReleaser](https://goreleaser.com). Tagging a
version cross-compiles the CLI for macOS/Linux (amd64 + arm64), publishes a
GitHub Release with prebuilt archives + checksums, and updates the Homebrew tap.

**One-time setup:**

1. Create a public tap repo **`gribovan2005/homebrew-drift`** (empty is fine).
2. Create a GitHub Personal Access Token with `repo` scope (classic) or
   `contents: read/write` on the tap (fine-grained), then add it to **this**
   repo as an Actions secret named **`HOMEBREW_TAP_GITHUB_TOKEN`**
   (Settings → Secrets and variables → Actions). The built-in `GITHUB_TOKEN`
   can't push to a second repo, hence the separate token.

**Cut a release:**

```bash
git tag v0.1.0
git push origin v0.1.0       # → .github/workflows/release.yml runs GoReleaser
```

After the workflow finishes, `brew install gribovan2005/drift/drift` works.
Dry-run the build locally (no publish) with:

```bash
goreleaser release --snapshot --clean --skip=publish
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
# drift
