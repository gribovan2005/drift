---
component: nexmark-coverage
status: complete
---

# Nexmark Coverage — all q0–q22 implemented ✅

Goal (achieved): implement every Nexmark query in Drift so we can benchmark the
full suite against Flink. See [[Benchmarks]] for numbers.

**Done.** All 23 queries run (`tests/nexmark`). New operators built:
`KeyedCountWindow` (group-by), `TopN`, `Join` (mixed-stream hash equi-join),
`sink.File`. The phased plan below is kept as the build record.

This is a stream-SQL-grade build. The work is gated by **four new operators** plus
a fuller event generator. Key architectural decision: **joins run as a single
operator over the mixed event stream** — the Nexmark generator already interleaves
Person / Auction / Bid events, so a join operator buffers the relevant types in
windowed state keyed by the join key and emits matches. No multi-source executor
change is needed (preserves the single-source, fixed-DAG model).

---

## Per-query map

| q | Query | Needs | Phase |
|---|---|---|---|
| q0 | pass-through | map | ✅ done |
| q1 | currency conversion | map | ✅ done |
| q2 | filter | filter | ✅ done |
| q7 | highest bid / tumbling window | global window **max** aggregate | 1 |
| q14 | expression / computed fields | map + filter | 1 |
| q21 | add channel id (from url) | map | 1 |
| q22 | split url into parts | map | 1 |
| q18 | latest bid per (auction,bidder) | dedup **keep-last** | 1 (small op) |
| q5 | hot items | keyed windowed **count** + **top-N** | 2 + 3 |
| q11 | user sessions per bidder | keyed **session count** | 2 |
| q12 | bids per bidder / proc-time window | keyed **group-by** (proc time) | 2 |
| q15 | bidding statistics (per day) | **group-by** + distinct | 2 |
| q16 | channel statistics | **group-by** + distinct | 2 |
| q17 | auction statistics | **group-by** + multi-agg | 2 |
| q19 | auction top-N bids | **top-N** per key | 3 |
| q3 | local item suggestion | **join** (auction⋈person) + filter | 4 |
| q8 | monitor new users | windowed **join** (person⋈auction) | 4 |
| q20 | expand bid with auction | **join** (bid⋈auction) | 4 |
| q13 | bounded side input | **join** vs static table | 4 |
| q4 | avg price per category | join + group-by + agg | 4 |
| q6 | avg selling price by seller | join + group-by + last-N | 4 |
| q9 | winning bids | join + group-by **max** | 4 |
| q10 | log to storage | **file sink** | 5 |

---

## Phases

### Phase 1 — generator + stateless/window-max queries
- Extend `tests/nexmark/events.go`: Person & Auction events + `GenerateEvents(n)`
  emitting the Nexmark **mixed stream** (Person:Auction:Bid ≈ 1:3:46), plus keep
  `GenerateBids` for the bid-only throughput queries.
- Implement q7 (global tumbling window, `max(price)` via a custom aggregate —
  existing `operator.NewTumblingWindow`), q14/q21/q22 (map), q18 (a small
  `dedup` keep-last variant or a `DeduplicateLast` op).
- Benchmarks + coverage update. Reaches ~8 queries.

### Phase 2 — keyed group-by aggregation (the big unlock)
- New `operator.KeyedAggregate(keyFn, windowing, aggFn)` — buffers per key within
  a window (count-based, processing-time tumbling, event-time tumbling, session),
  fires one record per (key, window). Aggregates: count, sum, avg, min, max,
  distinct-count. Reuses watermark patterns from `EventTimeWindow`/`SessionWindow`.
  Snapshottable + Flusher; parallelizable (key-partitioned).
- Implement q5(count part), q11, q12, q15, q16, q17. Reaches ~14 queries.

### Phase 3 — top-N / rank
- New `operator.TopN(keyFn, byFn, n, windowing)` — per-key top-N by a value within
  a window. Implement q19 and complete q5 (top hot items).

### Phase 4 — stream join (over the mixed stream)
- New `operator.Join(...)`: windowed equi-join keyed on a join field; buffers each
  side in event-time/processing-time windowed state; emits matches. A bounded
  variant joins against a static side table (q13).
- Implement q3, q8, q20, q13, then q4/q6/q9 (join composed with Phase-2 group-by).

### Phase 5 — file sink + full report
- New `sink.File` (NDJSON to a directory) for q10.
- Full `BENCHMARKS.md` table across all 22 + Flink comparison by category
  (stateless / windowed / join), with caveats.

---

## Invariants to keep

- Single source, fixed DAG (joins consume the mixed stream, not a second source).
- Spec → code → tests; `go test -race ./...` green per phase.
- New operators: `Snapshottable` for checkpointing, `Flusher` where they buffer,
  and parallelizable where keyed.
- Catalog + builder + YAML support for every new operator (palette stays in sync).

## See also

- [[Benchmarks]] · [[Operators]] · [[Control Plane]] · [[Overview#Parallelism]]
