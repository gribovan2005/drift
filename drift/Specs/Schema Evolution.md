---
component: schema-evolution
status: stable
package: pkg/schema
file: pkg/schema/registry.go
tested: true
---

# Schema Evolution

Live schema propagation — operators adapt to new schema versions without pipeline restart. Zero downtime.

## Problem

Flink requires stopping the job and migrating state on schema change. Drift instead pushes the new schema to all subscribed operators immediately at registration time.

---

## SchemaRegistry contract

```go
func NewRegistry() *Registry
func (r *Registry) Register(s core.Schema) error
func (r *Registry) Subscribe(id string, op core.Operator)
func (r *Registry) Version(id string, version int) (core.Schema, bool)
func (r *Registry) AllVersions(id string) []core.Schema
func (r *Registry) SchemaIDs() []string
```

**`Register` invariants:**
- New version must equal `len(existing) + 1` — returns error otherwise
- After storing, calls `OnSchemaChange(s)` on all subscribers for `id` **synchronously in registration order**
- Thread-safe: concurrent `Register` calls serialize internally

**`Subscribe` invariants:**
- An operator may subscribe to multiple schema IDs
- Subscribing after schemas exist does **not** replay history — caller must fetch current version manually if needed

---

## Operator requirements

Any operator that cares about schema **must**:

```go
type MyOp struct {
    mu     sync.RWMutex
    schema core.Schema
}

func (o *MyOp) OnSchemaChange(s core.Schema) {
    o.mu.Lock()
    defer o.mu.Unlock()
    o.schema = s
}

func (o *MyOp) Process(in []core.Record) ([]core.Record, error) {
    o.mu.RLock()
    schema := o.schema
    o.mu.RUnlock()
    // use schema ...
}
```

- `sync.RWMutex` is mandatory — `OnSchemaChange` is called from a different goroutine
- Never read `o.schema` without the read-lock

---

## SchemaAdapter

Built-in operator that normalises records to the current schema. Use this instead of writing field-handling logic in custom operators.

| Situation | Action |
|---|---|
| Field in record AND schema | Pass through, **coerced to `Field.Type`** |
| Field missing in record, present in schema | Insert `Field.Default` |
| Field in record, NOT in schema (removed) | Drop it |
| Field renamed (via `AliasMap`) | Read old name → write new name (coerced) |
| Field **type changed** between versions | Coerce the value to the new `Field.Type` |

```go
adapter := operator.NewSchemaAdapter(initialSchema, AliasMap{"amount": "value"})
registry.Subscribe("payments", adapter)
```

### Type coercion

A field's value is coerced to its declared `Field.Type`, so a **column type change**
between schema versions takes effect live (the same way add/remove/rename do). Rules
(best-effort, never panics, never drops data):

- numeric **widening is lossless** (`int`→`float`); `float`→`int` **truncates**
- `→string` uses `fmt`; `string`/`bool` are parsed when valid
- `bool`↔number: nonzero ⇄ true
- unparseable values, and `any`/`bytes`/untyped fields, **pass through unchanged**;
  `nil` stays `nil`

Demo: `go run ./cmd/schemademo` evolves `amount: int` → `amount_usd: float` (rename +
retype) and adds `region` **with no restart**.

---

## In-flight records

Records already in pipeline channels carry their original `SchemaVersion`. Operators **must not** assume incoming records match the current schema. Use `SchemaAdapter` upstream or handle via `registry.Version(id, record.SchemaVersion)`.

---

## Invariants

1. `Register` is the **only** way to advance a schema version
2. Version history is append-only — no downgrades ever
3. `OnSchemaChange` is called in subscriber registration order
4. Operators that don't call `Subscribe` are valid — they receive no notifications and process records as-is

---

## Required tests

| Test | Location | What it proves |
|---|---|---|
| `TestRegistry_SubscriberNotified` | `pkg/schema` | Subscriber receives all versions |
| `TestSchemaAdapter_LiveEvolution` | `pkg/operator` | Field add/remove/rename mid-stream |
| `TestSchemaAdapter_CoercesFieldTypes` | `pkg/operator` | Values coerced to each `Field.Type` |
| `TestSchemaAdapter_LiveRetype` | `pkg/operator` | Column type widened int→float mid-stream |
| `TestPipeline_LiveSchemaEvolution` | `tests/integration` | E2E: old-version records + new schema in parallel |

---

## See also

- [[Core Abstractions#Operator]]
- [[Operators#SchemaAdapter]]
- [[Overview#Execution model]]
