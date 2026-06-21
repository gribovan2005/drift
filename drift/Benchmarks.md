---
type: benchmarks
status: living
---

# Benchmarks — Nexmark vs Flink

[Nexmark](https://github.com/nexmark/nexmark) is the de-facto streaming benchmark
(auction system: Person / Auction / Bid events, queries q0–q22). We compare Drift
against Flink's **published** numbers on the queries Drift can express. Full
methodology + reproduce steps live in `BENCHMARKS.md` at the repo root; this is
the vault-side record + the plan to cover all queries.

## Headline result — SAME-IRON run (2026-06-20)

We ran **Flink 1.18 in Docker on this same laptop** (not published cloud numbers).
On the same machine, 50 M events, fair setup (on-the-fly gen + discard sink):

**Per core (P=1 / GOMAXPROCS=1):**

| Query | Flink | Drift | ratio |
|---|--:|--:|--:|
| q0 | 1.41 M/s | 1.40 M/s | ~1.0× |
| q1 | 693 K/s | 856 K/s | ~1.2× |
| q2 | 948 K/s | 1.40 M/s | ~1.5× |

**8-way on one box:** Flink q0 **2.48 M/s** vs Drift **883 K/s** — Flink scales
(parallel source subtasks), Drift's single source goroutine is the ceiling.

**Correction:** an earlier draft claimed ~4–5×; that was a hardware artifact
(Drift on fast Apple-M vs Flink's *published* slower-cloud numbers). Same-iron,
they're **comparable per core**, and Flink scales better on one multi-core box for
a single query. **Drift's real edge is operational** (single binary, no
JVM/cluster/shuffle, instant start, live schema evolution), not raw throughput.
Repro harness: `tests/nexmark` + `/tmp/flink-bench` (Flink SQL datagen→blackhole).

## Coverage status — 23 / 23 ✅ (full Nexmark suite)

All q0–q22 run on Drift. Operators built along the way:
- `operator.KeyedCountWindow` — group-by (count/sum/avg/max) → q5/q12/q15/q16/q17, q18 (keep-last), and the aggregate half of q4/q6/q9.
- `operator.TopN` — per-key/global top-N → q19, q5.
- `operator.Join` — hash equi-join over the **mixed** stream → q3/q8/q20 and the join half of q4/q6/q9; q13 via a static-table `Map`.
- `sink.File` — NDJSON file sink → q10.
- plus existing map/filter/`SessionWindow`/`TumblingWindow`.

Join + group-by queries (q4/q6/q9) run slower (~0.3–0.5 M/s) — join state +
aggregation — which is honest and expected. Pure stateless/window stay 1–2.5 M/s.

## See also

- [[Index]]
- [[Operators]]
- [[Overview#Parallelism]]
