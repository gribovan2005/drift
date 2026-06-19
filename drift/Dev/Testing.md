---
type: testing
status: stable
---

# Testing Requirements

`go test ./...` must pass before any PR merges. Race detector is always on in CI.

---

## Levels

| Level | Tool | Required for |
|---|---|---|
| Unit | `testing` + `testify/assert` | Every new operator or registry change |
| Integration | `testing` + in-memory source/sink | Every pipeline-level feature |
| Benchmarks | `testing.B` | Performance-sensitive code paths |
| Property-based | `testing/quick` | Operator invariants (idempotency, associativity) |

---

## Rules

**No mocks for SchemaRegistry.** Tests use a real in-process registry. Reason: mock divergence caused a past incident where tests passed but production schema propagation failed.

**Kafka tests are skipped without `KAFKA_ADDR`.** Never require a broker in the standard test run. Guard:
```go
broker := os.Getenv("KAFKA_ADDR")
if broker == "" {
    t.Skip("KAFKA_ADDR not set")
}
```

**Race detector always on in CI** (`go test -race ./...`). Any data race is a blocker.

---

## Operator tests

Every operator must have:
- `TestXxx_HappyPath` — basic transform works
- `TestXxx_ErrorPath` — fn/pred returning error propagates correctly
- `TestXxx_OnSchemaChange_Concurrent` — schema change races with Process; run with `-race`
- If stateful: `TestXxx_Flush_Partial` — partial batch emitted on Flush()

---

## Integration tests

Location: `tests/integration/`

Required for any pipeline-level feature. Must use:
- `source.NewMemory` as source
- `sink.NewMemory` as sink
- Real `SchemaRegistry` (never mock)

Key existing tests:
- `TestPipeline_BasicFlow` — records flow source → operators → sink
- `TestPipeline_LiveSchemaEvolution` — schema v1 records + v2 schema propagation, zero downtime

---

## Regression gate

`tests/bench/regression_test.go` contains `TestPipelineThroughputFloor`:
- **Minimum: 50,000 records/sec** on the CI machine
- If throughput drops >10%, investigate before merging
- Run locally: `go test -bench=. -benchmem ./tests/bench/`

Baseline throughputs (Apple M3):

| Workload | Records/sec |
|---|---|
| Filter (0 allocs/batch) | ~20M |
| Map | ~6M |
| Map+Filter pipeline | ~2.4M |
| TumblingWindow pipeline | ~7M |

---

## Commands

```bash
go test ./...                              # full suite
go test -race ./...                        # with race detector (CI default)
go test ./pkg/operator/...                 # unit only
go test ./tests/integration/...            # integration only
go test -bench=. ./tests/bench/...         # benchmarks
go test -bench=. -benchmem ./tests/bench/  # with allocations
```

---

## See also

- [[Workflow]]
- [[Operators#Adding a new operator]]
- [[Schema Evolution#Required tests]]
- [[AI Debugger#Required tests]]
