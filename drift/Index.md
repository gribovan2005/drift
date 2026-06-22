---
type: index
---

# Drift

Streaming data processing engine for Go. Single binary, zero external dependencies for the core.

**Two differentiators vs Flink:**
1. [[Schema Evolution]] — operators adapt to schema changes without pipeline restart
2. [[AI Debugger]] — Claude explains bottlenecks in plain language; not in the data path

---

## Architecture

- [[Overview]] — abstractions, data flow, non-goals
- [[Core Abstractions]] — Record, Schema, Operator, Source, Sink, Flusher

## Specs

- [[Schema Evolution]] — live schema propagation contract
- [[Operators]] — Map, Filter, FlatMap, SchemaAdapter, TumblingWindow, SlidingWindow
- [[Sources & Sinks]] — Memory, HTTP, Kafka
- [[Parallel Source]] — fan-in N sources / Kafka partitions to lift the single-reader ingestion ceiling
- [[SDK]] — single-import fluent facade for embedding Drift in Go services (subpackage `drift/sdk`)
- [[Resource Profiles]] — Sidecar/Dedicated tuning presets (batch/buffer/linger + opt-in runtime knobs)
- [[Vectorized Fast-Lane]] — columnar Batch + chunk-records + vectorized Map/Filter (`pkg/core` Batch, `pkg/vector`)
- [[Metrics Export]] — Prometheus text exposition over `Snapshot()` (`pkg/metrics`, `sdk.PrometheusHandler`, `GET /metrics`)
- [[AI Debugger]] — metrics → Claude → plain-language diagnosis
- [[CLI & Jobs]] — declarative YAML jobs + operator registry, `drift run/validate/graph/list`
- [[Lineage]] — record-level provenance (`Record.ID`/`Parents`, `pipeline.WithLineage`)
- [[Exactly-Once]] — WAL source replay + idempotent sink (`pkg/wal`, `Record.DeliveryKey`)
- [[Control Plane]] — runner + job store: build/save/run pipelines at runtime (`pkg/runner`, `drift serve`)
- [[Web UI & Builder]] — visual DAG builder + hardened dashboard (`pkg/web`)
- [[Nexmark Coverage]] — plan to implement all Nexmark queries q0–q22
- [[Distribution]] — Homebrew install via GoReleaser + tap (`.goreleaser.yaml`, `drift version`)

## Benchmarks

- [[Benchmarks]] — Nexmark vs Flink results + caveats (`tests/nexmark`, `BENCHMARKS.md`)

## Development

- [[Workflow]] — AI-driven dev: spec → code → test
- [[Conventions]] — code rules, error handling, concurrency
- [[Testing]] — levels, requirements, regression gates

---

## Module layout

| Package | Responsibility | May import |
|---|---|---|
| `pkg/core` | Interfaces only | — |
| `pkg/schema` | SchemaRegistry | core |
| `pkg/operator` | Built-in operators | core |
| `pkg/pipeline` | DAG executor | core, metrics, lineage |
| `pkg/lineage` | Record-level provenance tracker + operator decorator | core |
| `pkg/wal` | Write-ahead log + exactly-once coordinator (source/sink wrappers) | core, checkpoint |
| `pkg/runner` | Control plane: job store (YAML folder) + pipeline runner | job, pipeline, schema, dlq |
| `pkg/source` | Source implementations | core |
| `pkg/sink` | Sink implementations | core |
| `pkg/metrics` | OperatorMetrics | core |
| `pkg/ai` | Claude debugger | metrics, core |
| `pkg/web` | SSE server + embedded UI + builder/control-plane API | pipeline, ai, schema, runner, job, dlq |
| `pkg/job` | YAML job spec + operator/source/sink registry + loader | core, operator, source, sink, pipeline |
| `sdk` | Fluent SDK facade for embedding | core, operator, source, sink, pipeline, checkpoint, lineage |
| `cmd/drift` | CLI: run/validate/graph/list/serve | all |
| `cmd/demo` | Demo pipeline + web UI | all |
| `cmd/sdkdemo` | SDK demo: real-time analytics embedded in a Go HTTP service (live view + schema evolution + `/metrics`) | sdk, operator, schema |

**Import rule**: `pkg/core` must never import other `pkg/` packages.
