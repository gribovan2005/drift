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

Three levels, all within one process:

- **Pipeline (inter-stage)** — every stage is its own goroutine; all stages run
  concurrently across cores (an assembly line). Fan-out branches also run in
  parallel. This is always on.
- **Intra-stage (data) parallelism** — a stage can opt into `parallelism: N` to
  shard its own load across N goroutines via `pipeline.Parallel`: stateless ops
  shard round-robin; keyed ops (dedup, session) shard by key (correctness kept);
  global windows (tumbling, eventwindow) cannot be sharded. The executor is
  unchanged — `Parallel` is an operator decorator. See [[Operators]].
- **Inter-pipeline (N-lane)** — `pipeline.RunLanes` / `sdk.RunLanes` run N fully
  independent pipelines (own source + sink each, no shared channel), the
  task-per-partition model. Highest single-node scaling (no funnel); fail-fast. Pair
  with the parallel triad (parallel source decode + `vector.Parallel` + `sink.Parallel`)
  inside each lane. Keyed aggregation across lanes is global-correct without
  key-sharding: each lane runs a partial `vector.GroupBy`, then `Group.MergeOp` folds
  the partials into the global result. See [[Benchmarks]] (Lanes/MaxLane/DistGroupBy).

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

### Library, distribution & performance

The pivot to "embed Drift in a Go service" and "be the fastest single-node Go
stream engine". All shipped:

| Feature | Status | Notes |
|---|---|---|
| **Embeddable Go SDK** | ✅ done | root `drift/sdk` fluent facade (`sdk.New().From().Map().To().Run()`); module is `go get`-able. See [[SDK]] |
| **Homebrew distribution** | ✅ done | `brew install gribovan2005/drift/drift`; GoReleaser + tap, single self-contained binary. See [[Distribution]] |
| **Prometheus metrics export** | ✅ done | dependency-free text exposition over `pipeline.Snapshot()`; `sdk.PrometheusHandler`, auth-exempt `GET /metrics`. See [[Metrics Export]] |
| **Resource profiles** | ✅ done | `Sidecar`/`Dedicated` presets (batch/buffer/linger + opt-in process-global knobs); SDK (`WithProfile`) **and** YAML/runner (`profile:` field). See [[Resource Profiles]] |
| **Parallel triad (source/stage/sink)** | ✅ done | `source.NewParallel` (+ Kafka partition readers) for decode, `vector.Parallel` for compute, `sink.Parallel` for egress — every serial point parallel; full columnar lane scales ~5.4× @8 (MaxLane). See [[Parallel Source]], [[Benchmarks]] |
| **Vectorized fast-lane** | ✅ done | columnar `core.Batch` carried as chunk-records; Int64/Float64/String/Bool `Map`/`Filter` + global `Sum`/`Count`/`Max` + **keyed `GroupBy`** (+ **distributed across lanes** via partials + `MergeOp`, no key-sharding needed) + **event-time `TumblingGroup`** (watermark, periodic emit) + **build-side `HashJoin`** (dimension enrichment); binary codec (all four kinds) + `KafkaColumnarSource`; `vector.Parallel` per-stage scaling; row-accurate metrics. **~247× on the stateless hot path, ~24× on group-by, ~52M rows/s over real Kafka.** See [[Vectorized Fast-Lane]], [[Benchmarks]] |

### Next (not yet built — explicit scope, not bugs)

- **Vectorized stream-stream joins + sliding/session windows** — columnar
  Map/Filter, global + keyed `GroupBy`, event-time `TumblingGroup`, and build-side
  `HashJoin` (dimension enrichment) are done; **stream-stream (mixed) joins**,
  left-outer/M:N joins, and sliding/session windows stay on the row engine.
- **Left-outer / M:N joins** (need NULL columns) and **sliding/session** columnar
  windows remain on the row engine.
- **Copy-on-fan-out for chunks** — vectorized ops mutate batches in place (safe for
  linear pipelines only); a branching DAG sharing a chunk needs a copy.
- **Non-linear DAG in the SDK builder** — the row engine supports DAGs via
  `Stage.Next`/YAML; the fluent SDK builds linear chains only.

(Horizontal scale-out with coordinated partitioned state + rebalance remains a
**permanent non-goal** for the core — that's Flink/Kafka-Streams territory; run N
independent partition-pipelines instead. See [[Benchmarks]].)

## See also

- [[Core Abstractions]]
- [[Schema Evolution]]
- [[Workflow]]
