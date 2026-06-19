---
type: conventions
status: stable
---

# Code Conventions

Enforced for all code in this repository. Agents must follow these without exception.

---

## Comments

- Write comments only for **non-obvious WHY** — hidden constraint, subtle invariant, workaround for a specific bug
- Never describe WHAT the code does — well-named identifiers do that
- No multi-line docstrings; one short line max
- Never reference the current task, issue number, or caller — those belong in the PR description

```go
// Bad: Process transforms the input records using the map function
// Good: (no comment — the function name says it)

// Bad: // added for the fraud-detection PR
// Good: (delete it)

// Good: // RLock here because OnSchemaChange can race with Process
```

---

## Errors

Wrap with context:
```go
return fmt.Errorf("schema registry: %w", err)
return fmt.Errorf("operator %s: process: %w", o.label, err)
```

- Component name first, then operation, then `%w`
- Never swallow errors silently
- Don't add error handling for scenarios that can't happen — trust internal invariants

---

## Concurrency

- Every goroutine has a clear owner and a shutdown path via `context.Context`
- `sync.RWMutex` for any field read by `Process` and written by `OnSchemaChange`
- Never use `sync.Mutex` where `sync.RWMutex` suffices — `Process` is on the hot path

```go
// Pattern for all schema-aware operators:
type MyOp struct {
    mu     sync.RWMutex
    schema core.Schema
}
func (o *MyOp) OnSchemaChange(s core.Schema) {
    o.mu.Lock(); defer o.mu.Unlock()
    o.schema = s
}
func (o *MyOp) Process(in []core.Record) ([]core.Record, error) {
    o.mu.RLock(); schema := o.schema; o.mu.RUnlock()
    // use schema — not o.schema
}
```

---

## No unnecessary abstractions

- A bug fix doesn't need surrounding cleanup
- A one-shot operation doesn't need a helper
- Three similar lines is better than a premature abstraction
- No half-finished implementations — implement exactly what the spec says

---

## No backwards-compatibility hacks

- If something is unused, delete it completely
- No `_` variable renames, re-exports of removed types, or `// removed` comments
- Rename confidently — the spec and tests are the contract, not the symbol name

---

## Imports

- `pkg/core` imports nothing else in `pkg/`
- Group: stdlib / external / internal (goimports order)
- No dot imports

---

## See also

- [[Workflow]]
- [[Testing]]
- [[Core Abstractions#Operator]]
