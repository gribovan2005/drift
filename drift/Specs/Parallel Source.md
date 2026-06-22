---
component: parallel-source
status: stable
package: pkg/source, sdk
tested: true
---

# Parallel Source

A `core.Source` reads on a single goroutine, so one Kafka topic (consumer-group
reader) or one generator is a serial **ingestion ceiling** — adding pipeline cores
can't speed it up (see [[Benchmarks]]). `Parallel` lifts that ceiling: it runs N
sub-sources concurrently and fans their records into one stream, so N Kafka
**partitions** (or any N sources) are consumed in parallel.

```go
// any N sources → one parallel stream
src := source.NewParallel(s0, s1, s2)

// N partition-pinned Kafka readers of one topic, fanned in
src := source.KafkaPartitions(cfg, []int{0, 1, 2, ... 9})
```

It implements `core.Source`, so it drops straight into `pipeline.New(src, ...)` /
`sdk.New().From(src)`.

---

## API

```go
// pkg/source
func NewParallel(subs ...core.Source) *Parallel        // generic fan-in
func KafkaPartitions(cfg KafkaConfig, partitions []int, opts ...KafkaOption) core.Source

type KafkaConfig struct {
    // ... existing fields ...
    Partition int // used only when GroupID == "" (partition-pinned reader)
}

// sdk (re-exports, compose with the fluent API)
func ParallelSource(subs ...Source) Source
func KafkaPartitions(cfg source.KafkaConfig, partitions []int) Source
```

### Kafka: group vs partition reader

- `GroupID != ""` → consumer-group reader (Kafka auto-assigns partitions across
  reader instances). Unchanged default.
- `GroupID == ""` → partition-pinned reader on `Partition` (kafka-go forbids
  setting both). `KafkaPartitions` builds one such reader per partition and wraps
  them in `NewParallel`. Use this to saturate ingestion within one process.

---

## Semantics

1. `Read(ctx)` calls each sub-source's `Read`, then fans every sub-channel into one
   output channel via a goroutine per sub (the same fan-in pattern as
   `pipeline.mergeAll`). The output closes when **all** subs drain or ctx is done.
2. **Cross-source ordering is not preserved** — records interleave by arrival, just
   like reading across Kafka partitions. Keyed/stateful operators downstream must
   shard by key (e.g. `pipeline.Parallel` with a key func), not rely on input order.
3. Respects ctx cancellation — every fan-in goroutine exits on `ctx.Done()` and the
   output channel is closed.
4. Empty (`NewParallel()` with no subs) → an immediately-closed channel.
5. If a sub-source's `Read` returns an error, `Parallel.Read` returns it (config-
   time failure, e.g. no brokers).

### Throughput note (honest)

Parallel ingestion removes the single-reader ceiling, but funnelling N readers into
**one** channel + one downstream stage just moves the bottleneck. Real scaling
needs the downstream to scale too: pair `Parallel` with `pipeline.Parallel` on hot
stages and/or run the vectorized fast-lane (see [[Vectorized Fast-Lane]]). Alone it
helps ingestion-bound pipelines.

---

## Required tests (no mocks; real in-process; `-race` green)

- `Parallel_FansInAll` — 3 in-memory sub-sources with disjoint records → all
  records arrive exactly once (count + set), order-agnostic.
- `Parallel_RespectsCancel` — a never-closing sub (generator) → output channel
  closes after ctx cancel, no goroutine leak/hang.
- `Parallel_Empty` — no subs → closed channel, no panic.
- Kafka partition reader — guarded by `KAFKA_ADDR` (skipped otherwise, like the
  existing Kafka tests).

---

## Parallel sink (the egress counterpart)

`sink.Parallel(n, mk func() core.Sink) core.Sink` (re-exported as `sdk.ParallelSink`)
mirrors `NewParallel` on the egress side: it fans the final stream **round-robin** to
n inner sinks, each on its own goroutine, removing the single-sink serial point
(`pkg/pipeline/pipeline.go` runs the sink on one goroutine). With a parallel source +
`vector.Parallel` stage + parallel sink, a fully-columnar lane scales across cores.

- Round-robin distribution suits **stateless** sinks; a stateful/keyed sink must be
  sharded by key upstream instead.
- An inner-sink error (or parent ctx cancel) cancels a child context so the
  dispatcher unblocks; the first inner error is returned.
- Both engine edges still pass through one channel each — for large columnar chunks
  that's cheap (few items), so the parallel sink lifts the cap; for tiny row records
  the per-record channel cost dominates. See [[Benchmarks]] (MaxLane).
- Tests: fan-out completeness, n=1 passthrough, ctx cancel, inner-error propagation.

---

## See also

- [[Sources & Sinks]] — the underlying source implementations
- [[Vectorized Fast-Lane]] — the columnar path that scales the *downstream* so
  parallel ingestion actually pays off
- [[Benchmarks]] — the single-source ceiling this addresses
