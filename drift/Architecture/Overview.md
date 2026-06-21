---
component: architecture
status: stable
---

# Architecture Overview

## Data flow

```
Source → Stage[0] → Stage[1] → … → Stage[n] → Sink
          goroutine  goroutine         goroutine
```

Channels between stages are **buffered (256 records)**. Batch size per `Process` call: **64 records**. Backpressure is implicit — upstream goroutine blocks when downstream channel is full.

## Execution model

- Each stage runs in its own goroutine, owned by `pkg/pipeline`
- Pipeline shuts down by cancelling the `context.Context` passed to `Run()`
- When source closes, pipeline calls `Flush()` on stages that implement `core.Flusher` (e.g. windows with partial batches)
- Goroutine lifecycle: source goroutine → stage goroutines → sink goroutine, each waits for upstream channel close

## Key design decisions

| Decision | Choice | Why |
|---|---|---|
| Backpressure | Implicit (channel blocking) | Zero overhead, no extra signalling |
| Schema propagation | Push via `OnSchemaChange` | Operators react immediately, no polling |
| Metrics | Pull via `Snapshot()` | Decoupled from data path, no latency impact |
| AI analysis | On-demand REST call | Not in hot path, operator has full context |
| UI updates | SSE (not WebSocket) | No dependency, simpler client |

## Parallelism

Two levels, both within one process:

- **Pipeline (inter-stage)** — every stage is its own goroutine; all stages run
  concurrently across cores (an assembly line). Fan-out branches also run in
  parallel. This is always on.
- **Intra-stage (data) parallelism** — a stage can opt into `parallelism: N` to
  shard its own load across N goroutines via `pipeline.Parallel`: stateless ops
  shard round-robin; keyed ops (dedup, session) shard by key (correctness kept);
  global windows (tumbling, eventwindow) cannot be sharded. The executor is
  unchanged — `Parallel` is an operator decorator. See [[Operators]].

## Non-goals (permanent)

- Distributed cluster execution — single binary, single process, always
- SQL layer
- Dynamic DAG reshaping at runtime (DAG is fixed at pipeline construction)

## Production roadmap (Path A — single-process, production-grade)

Prioritised by dependency order:

| # | Feature | Status | Depends on | Notes |
|---|---|---|---|---|
| 1 | **True DAG executor** | ✅ done | — | Per-edge buffered channels, `broadcastAll` fan-out, `mergeAll` fan-in; backward-compatible with linear pipelines |
| 4 | **State backend (BadgerDB)** | ✅ done | — | `pkg/checkpoint.BadgerStore` — embedded KV store, pluggable via the `Store` interface, survives restarts |
| 2 | **Event time + watermarks** | ✅ done | DAG executor | `EventTime time.Time` on `core.Record`; `TimestampAssigner` + `EventTimeWindow` operators; bounded-out-of-orderness watermark computed per operator |
| 3 | **Session windows** | ✅ done | Event time | `operator.SessionWindow` — keyed, gap-based; fires when watermark passes `sessionMax + gap`; merges bridged sessions |
| 5 | **Exactly-once via WAL** | ✅ done | State backend | `pkg/wal` — durable append-only log replays un-committed records on restart; idempotent sink dedups by stable `DeliveryKey`; commit watermark advances only after sink ack. Exact for passthrough, at-least-once through aggregation. See [[Exactly-Once]] |

**Goal:** single-binary drop-in for teams that need production reliability without operating a Flink cluster. **Path A complete.**

### Operability (beyond Path A)

| Feature | Status | Notes |
|---|---|---|
| **CLI + declarative jobs** | ✅ done | `pkg/job` + `cmd/drift` — YAML jobs (hybrid built-ins + `ref:` registry), `drift run/validate/graph/list`. See [[CLI & Jobs]] |
| **Record-level lineage** | ✅ done | `pkg/lineage` — per-stage record IDs + parent graph via `pipeline.WithLineage`; **exact** for all built-ins, including per-window provenance for aggregating windows. See [[Lineage]] |
| **Control plane + visual builder** | ✅ done | `pkg/runner` (job store over a YAML folder + runtime pipeline runner) + `pkg/web` builder. Build a DAG visually, save/load YAML, run/stop from the UI. Rebuild-per-run + pointer swap under `RWMutex` (DAG stays immutable). `drift serve`. See [[Control Plane]], [[Web UI & Builder]] |

## See also

- [[Core Abstractions]]
- [[Schema Evolution]]
- [[Workflow]]
