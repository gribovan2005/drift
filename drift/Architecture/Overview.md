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

## Non-goals (MVP)

- Distributed execution — single process only
- Exactly-once semantics — at-least-once only
- Persistent state (RocksDB)
- Event time / watermarks
- Session windows
- SQL layer
- Dynamic DAG reshaping at runtime

## See also

- [[Core Abstractions]]
- [[Schema Evolution]]
- [[Workflow]]
