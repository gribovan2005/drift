---
component: operators
status: stable
package: pkg/operator
tested: true
---

# Operators

All operators live in `pkg/operator` and implement `core.Operator`. See [[Core Abstractions#Operator]] for the interface contract.

---

## Map

```go
NewMap(fn MapFunc) *Map
type MapFunc func(core.Record) (core.Record, error)
```

- 1-to-1 transform
- If `fn` returns an error, `Process` halts the batch and returns the error
- `OnSchemaChange` stores schema for use in `fn` (pass via closure)

**Invariant:** output record count == input record count

---

## Filter

```go
NewFilter(pred PredicateFunc) *Filter
type PredicateFunc func(core.Record) bool
```

- Passes only records where `pred` returns `true`
- Zero allocations when all records pass (reuses input slice)
- **Idempotent**: `Filter(Filter(x)) == Filter(x)`

---

## FlatMap

```go
NewFlatMap(fn FlatMapFunc) *FlatMap
type FlatMapFunc func(core.Record) ([]core.Record, error)
```

- 1-to-N (or 1-to-0) transform
- `return nil, nil` drops the record — equivalent to filter-out
- Output count is not bounded

---

## SchemaAdapter

```go
NewSchemaAdapter(initial core.Schema, aliases AliasMap) *SchemaAdapter
type AliasMap map[string]string // oldName → newName
```

- Normalises records to the current schema (full rules in [[Schema Evolution#SchemaAdapter]])
- Thread-safe: `OnSchemaChange` holds write-lock; `Process` holds read-lock
- Must be placed **before** any operator that assumes a specific schema shape

---

## TumblingWindow

```go
NewTumblingWindow(size int, fn AggregateFunc) (*TumblingWindow, error)
type AggregateFunc func(window []core.Record) (core.Record, error)
```

- Collects exactly `size` records, then emits one aggregate record
- Implements `core.Flusher` — pipeline calls `Flush()` on source close to emit partial window
- `size` must be ≥ 1

**State:** internal buffer of up to `size` records. Protected by mutex.

---

## SlidingWindow

```go
NewSlidingWindow(size, step int, fn AggregateFunc) (*SlidingWindow, error)
```

- Every `step` records: emits aggregate of the last `size` records
- Windows overlap when `step < size`; equivalent to tumbling when `step == size`
- Implements `core.Flusher` — emits partial step on source close
- Constraints: `size ≥ step ≥ 1`

---

## Adding a new operator

Follow `skills/add-operator.md`. Required checklist:
- [ ] Spec in `specs/operators.md` (and here in this note)
- [ ] Implementation in `pkg/operator/<name>.go`
- [ ] `OnSchemaChange` uses `sync.RWMutex` if operator reads schema in `Process`
- [ ] Unit tests: happy path, error path, `OnSchemaChange` concurrent with `Process`
- [ ] If stateful: implements `core.Flusher` + flush test

---

## Planned (post-MVP)

| Operator | Description |
|---|---|
| `Merge` | Combines two input channels into one |
| `Split` | Routes records to N channels by routing function |
| `Deduplicate` | Drops duplicate records within a time window |

---

## See also

- [[Core Abstractions#Flusher]]
- [[Schema Evolution#SchemaAdapter]]
- [[Testing#Operator tests]]
