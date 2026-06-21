---
component: core
status: stable
package: pkg/core
file: pkg/core/core.go
---

# Core Abstractions

`pkg/core` defines all interfaces. **Never imports other `pkg/` packages.**

---

## Record

The fundamental unit of data flowing through the pipeline.

```go
type Record struct {
    SchemaID      string
    SchemaVersion int
    Payload       map[string]any
    EventTime     time.Time // when the event occurred (zero = unset)
    ID            string    // unique instance id for lineage (empty = lineage off)
    Parents       []string  // ids this record was derived from (empty for roots)
    DeliveryKey   string    // stable across replays for exactly-once (empty = EOS off)
}
```

**Invariants:**
- `SchemaID` must match a registered schema in [[Schema Evolution|SchemaRegistry]]
- `SchemaVersion` is the version at the time the record was produced
- Records in-flight may have an older `SchemaVersion` than the current schema — operators must handle this
- `EventTime` is the **event time** (when the event happened at the source), distinct from processing time (when Drift sees it). Zero value means unset — populate it with [[Operators#TimestampAssigner|TimestampAssigner]] before any event-time window. Event-time operators ignore records whose `EventTime` is zero only if documented; otherwise a zero time sorts before all real events.
- `ID`/`Parents` carry record-level provenance and are populated only when lineage is enabled (`pipeline.WithLineage`); otherwise they stay empty with zero overhead. See [[Lineage]].
- `DeliveryKey` is a replay-stable identifier for exactly-once delivery — set by the WAL source (`wal:<LSN>`), read by the idempotent sink to dedup replayed records. Empty when exactly-once is off, zero overhead. See [[Exactly-Once]].

---

## Watermarks (event time)

A **watermark** is a monotonic estimate of "event-time progress": a claim that no
record with `EventTime` ≤ watermark will arrive later. Drift uses the standard
**bounded-out-of-orderness** strategy:

```
watermark = maxEventTimeSeen − allowedLateness
```

- `allowedLateness` is the operator's tolerance for out-of-order events.
- A record is **late** when its window has already closed relative to the current
  watermark (`windowEnd ≤ watermark`). Late records are dropped and counted.
- **Single-process model:** the watermark is computed *inside* each event-time
  operator from the `EventTime`s it observes — there is no separate watermark
  event threaded through the channels. Because records flow downstream carrying
  their `EventTime`, each downstream event-time operator recomputes its own
  watermark. This keeps the DAG executor unchanged.
- On stream end, `Flush()` advances the watermark to +∞ so all pending windows fire.

See [[Operators#EventTimeWindow|EventTimeWindow]].

---

## Schema

```go
type Schema struct {
    ID      string
    Version int
    Fields  []Field
}

type Field struct {
    Name     string
    Type     string
    Default  any
    Nullable bool
}
```

**Invariants:**
- Versions are linear: 1, 2, 3 … — no gaps, no downgrades
- `Field.Default` is used by [[Operators#SchemaAdapter|SchemaAdapter]] when adding fields to old records

---

## Operator

```go
type Operator interface {
    Process(in []Record) ([]Record, error)
    OnSchemaChange(s Schema)
}
```

**Contract:**
- `Process` is called by the pipeline executor per batch (≤64 records)
- `OnSchemaChange` is called by SchemaRegistry from a **different goroutine** than `Process` — implementations **must** use `sync.RWMutex`
- After `OnSchemaChange` returns, the **next** `Process` call must use the new schema
- `Process` returning an error stops the stage and propagates to the pipeline

---

## Flusher

```go
type Flusher interface {
    Flush() ([]Record, error)
}
```

Implemented by stateful operators ([[Operators#TumblingWindow|TumblingWindow]], [[Operators#SlidingWindow|SlidingWindow]]). The pipeline calls `Flush()` on upstream close to emit partial batches.

---

## Source

```go
type Source interface {
    Read(ctx context.Context) (<-chan Record, error)
}
```

- Returns a channel that closes when the source is exhausted or `ctx` is cancelled
- The pipeline owns the channel; Source must not close channels it did not create

---

## Sink

```go
type Sink interface {
    Write(ctx context.Context, ch <-chan Record) error
}
```

- Drains `ch` until closed or `ctx` is cancelled
- Returns nil on clean shutdown, error on failure

---

## See also

- [[Overview]]
- [[Schema Evolution]]
- [[Operators]]
- [[Sources & Sinks]]
