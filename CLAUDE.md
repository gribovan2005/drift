# Drift — Claude Code Bootstrap

## Source of truth

All specs, architecture, conventions, and workflow rules live in the Obsidian vault:

```
drift/Index.md          ← start here
drift/Architecture/     ← abstractions, data flow, design decisions
drift/Specs/            ← component contracts, invariants, test requirements
drift/Dev/              ← workflow, conventions, testing rules
```

**Before implementing anything: read the relevant spec in `drift/Specs/`.**

---

## Hard rules (memorise these)

1. `pkg/core` never imports other `pkg/` packages
2. `sync.RWMutex` is mandatory in any operator that reads schema in `Process` — `OnSchemaChange` runs on a different goroutine
3. No mocks for SchemaRegistry — use real in-process registry in tests
4. No PR without `go test ./...` passing
5. Spec → Code → Tests — in that order, no shortcuts

---

## Running the project

```bash
go run ./cmd/demo           # demo pipeline + web UI on :8080
go test ./...               # full test suite
go test -race ./...         # with race detector (CI default)
go test -bench=. ./tests/bench/...  # benchmarks
```

AI debugging requires `ANTHROPIC_API_KEY` in `.env`.
