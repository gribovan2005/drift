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
}
```

**Invariants:**
- `SchemaID` must match a registered schema in [[Schema Evolution|SchemaRegistry]]
- `SchemaVersion` is the version at the time the record was produced
- Records in-flight may have an older `SchemaVersion` than the current schema — operators must handle this

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
