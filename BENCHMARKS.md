# Drift Benchmarks — Nexmark

[Nexmark](https://github.com/nexmark/nexmark) is the de-facto streaming benchmark
(an auction system: Person / Auction / Bid events, queries q0–q22) used to
measure engines like Apache Flink.

> **TL;DR (corrected by a same-iron run).** On the **same machine**, Drift and
> Flink are **comparable per core** on stateless queries (Drift ~1.0–1.5×) — *not*
> the ~4–5× an earlier draft claimed. That gap was a hardware artifact: Drift ran
> on a fast Apple-M laptop while Flink's *published* numbers are from slower cloud
> iron. When we actually ran Flink **on this laptop**, it hit 0.7–1.4 M/s per core
> too. And on one multi-core box Flink **scales better** for a single query (it
> parallelizes its source across subtasks; Drift's single source goroutine is a
> ceiling). **Drift's real edge is operational** — single binary, no JVM/cluster/
> shuffle, instant start, live schema evolution — not raw throughput superiority.

---

## Results

**Drift** — single process, 2,000,000 bid events, `map[string]any` payloads (7
fields), no checkpointing. Apple M-series, Go 1.26. Single representative run.

| Query | What it does | Drift, 1 core | Drift, 8 cores |
|---|---|---:|---:|
| q0 | pass-through (ceiling) | **810 K/s** | 1.1 M/s |
| q1 | currency conversion (map) | **684 K/s** | 800 K/s |
| q2 | filter (auction % 123 == 0) | **2.9 M/s** | 1.6 M/s |

(q2 drops ~99% of the stream, so the sink does almost nothing — its rate is
dominated by the filter and is noisy run-to-run.)

## Same-iron run (the trustworthy comparison)

Both engines on **the same machine** (Apple M, 8 cores, 8 GB), **50 M events**,
fair setup: on-the-fly generation + discard sink (Flink `datagen`→`blackhole`,
Drift bounded source→discard). Flink 1.18.1 in Docker; throughput from the REST
job runtime (excludes client startup). Drift = wall time of `Run`. Single run.

**Per core (parallelism = 1 / `GOMAXPROCS=1`):**

| Query | Flink | Drift | ratio (Drift/Flink) |
|---|---:|---:|---:|
| q0 pass-through | 1.41 M/s | 1.40 M/s | ~1.0× |
| q1 currency map | 693 K/s | 856 K/s | ~1.2× |
| q2 filter | 948 K/s | 1.40 M/s | ~1.5× |

→ **Comparable per core.** Drift edges ahead on map/filter (no codec, no object
churn), but it's the same order of magnitude — the "4–5×" was purely Drift-on-fast
-Mac vs Flink-on-cloud.

**Scaling on one box (8-way):**

| Query | Flink P=8 | Drift `GOMAXPROCS=8` |
|---|---:|---:|
| q0 | 2.48 M/s | 883 K/s |
| q1 | 923 K/s | 871 K/s |
| q2 | 1.51 M/s | 1.30 M/s |

→ **Flink scales up, Drift (single pipeline) does not.** Flink runs parallel
source subtasks; Drift's one source goroutine + one stage goroutine is the ceiling
for a single-stage query (`pipeline.Parallel` speeds a *stage's* compute, not
ingestion). Flink's scale-up here is also sublinear — the 8 GB box throttles it.

(Earlier published Flink Nexmark numbers — ~155 K/s/core on 100 M events — reflect
slower hardware and are *not* used for the comparison above; they only show why a
cross-hardware ratio is meaningless.)

### Honest verdict

Per core, same iron: **a wash** (Drift ~1.0–1.5× on stateless queries). For a
single query on one multi-core box, **Flink scales better**. Drift wins on
**operational simplicity** (one binary, no JVM/cluster/Zookeeper/shuffle, ~instant
start vs Flink's per-job deploy), plus its differentiators (live schema evolution,
AI debugger). It is *not* a raw-throughput giant-killer — and for &gt;1-machine
scale or huge state, Flink is the right tool (see the 10 TB discussion).

### Full suite — all 23 queries (Drift, 2M events, single representative run)

| q | category | events/sec | | q | category | events/sec |
|---|---|--:|---|---|---|--:|
| q0 | stateless | 1.0 M/s | | q12 | group-by | 1.4 M/s |
| q1 | stateless | 840 k/s | | q13 | static join | 941 k/s |
| q2 | stateless | 2.5 M/s | | q14 | stateless | 600 k/s |
| q3 | join | 1.5 M/s | | q15 | group-by | 1.1 M/s |
| q4 | join+group | 456 k/s | | q16 | group-by | 1.9 M/s |
| q5 | group+top-N | 1.9 M/s | | q17 | group-by | 1.8 M/s |
| q6 | join+group | 451 k/s | | q18 | keep-last | 1.7 M/s |
| q7 | window max | 2.5 M/s | | q19 | top-N | 1.0 M/s |
| q8 | join | 1.5 M/s | | q20 | join | 263 k/s |
| q9 | join+max | 297 k/s | | q21 | stateless | 225 k/s |
| q10 | file sink | 302 k/s | | q22 | stateless | 269 k/s |
| q11 | session | 231 k/s | | | | |

Pattern: stateless / window / group-by queries run **1–2.5 M/s**; join+aggregate
(q4/q6/q9/q20) and event-time/IO (q10/q11) run **0.2–0.5 M/s** — the join state,
aggregation, watermarks, and disk writes cost real CPU, as expected. Numbers are
single noisy runs (q21/q22 vary widely run-to-run); reproduce and take medians.

---

## Caveats (read before quoting)

1. **Different hardware.** Flink's numbers are from the official Nexmark run on
   their setup; Drift ran on an Apple M-series laptop. Per-core normalizes
   *some* of this but **not all** — this is indicative, not same-iron. A rigorous
   claim needs Flink run on the same machine.
2. **Subset of queries.** Drift only implements the **shuffle-free** queries
   (q0/q1/q2) — exactly where the no-shuffle thesis wins most. The hard queries
   (joins, group-by) are not implemented and would narrow or erase the gap.
3. **In-memory generator.** Drift pre-materializes events; Flink's figure
   includes source generation. This favours Drift somewhat.
4. **Single-run, noisy.** Especially q2 (1.6–2.9 M/s across runs). Reproduce and
   take a median for anything you publish.
5. **No fault tolerance in the hot path** for these runs (Flink Nexmark
   throughput runs typically also disable checkpointing — roughly fair).

Honest headline for a pitch: *"On the stateless Nexmark queries, Drift delivers
several× Flink's per-core throughput in a single binary — no cluster, no shuffle.
We haven't yet built the operators (join, group-by) the remaining queries need."*

---

## Nexmark coverage map (q0–q22)

What Drift can express today, and what each remaining query needs.

| Query | Needs | Status |
|---|---|---|
| q0 pass-through | map | ✅ implemented |
| q1 currency conversion | map | ✅ implemented |
| q2 filter | filter | ✅ implemented |
| q7 highest bid per window | windowed max | ✅ implemented (`TumblingWindow` max aggregate) |
| q14 expression + filter | map | ✅ implemented |
| q21 add channel id | map | ✅ implemented |
| q22 split url | map | ✅ implemented |
| q18 dedup latest per key | dedup keep-last | ◻︎ needs a keep-last dedup variant |
| q12 bids per bidder / window | keyed **group-by** count | ✅ implemented (`KeyedCountWindow`) |
| q15 bidding stats per day | **group-by** count | ✅ implemented |
| q16 channel stats | **group-by** count | ✅ implemented |
| q17 auction stats | **group-by** count | ✅ implemented |
| q5 hot items | group-by + top-N | ✅ `KeyedCountWindow` → `TopN` |
| q11 user sessions per bidder | keyed session count | ✅ `SessionWindow` |
| q18 latest per (auction,bidder) | windowed keep-last | ✅ `KeyedCountWindow` last-agg |
| q19 top-10 bids per auction | top-N | ✅ `TopN` |
| q3 / q8 / q20 | stream **join** | ✅ `Join` (over the mixed stream) |
| q4 / q6 / q9 | join + group-by | ✅ `Join` → `KeyedCountWindow` (avg/max) |
| q13 bounded side input | static join | ✅ `Map` over a side table |
| q10 log to storage | file sink | ✅ `sink.File` (NDJSON) |

**Progress: 23 / 23 implemented — full Nexmark suite.** New operators across the
build: `KeyedCountWindow` (group-by), `TopN`, `Join` (mixed-stream hash equi-join),
`sink.File`. See `drift/Specs/Nexmark Coverage.md`.

**The unlocks, in priority order:**
1. **Keyed group-by aggregation** (windowed + global) → opens q5, q7, q11, q12,
   q15–q17. Biggest single win.
2. **Stream join** → q3, q4, q6, q8, q9, q13, q20.
3. **Top-N / rank** → q5, q19.

---

## Vectorized fast-lane — the record format was the wall

The row engine stores each value in a `map[string]any` (boxed + GC-scanned). The
columnar fast-lane (`pkg/vector` + `core.Batch`) stores typed columns
(`[]int64`/`[]float64`) and processes them in tight loops — no map, no boxing, no
per-row alloc. Same logical workload — `Filter(even)` then `Map(+1)` over 5M ints,
`GOMAXPROCS=1` (`tests/bench/vector_bench_test.go`):

| path | rows/sec |
|---|---:|
| row (`map[string]any`) | 1.20 M/s |
| **vec (columnar Int64)** | **296 M/s** (~247×) |

→ Changing only the representation is ~**two orders of magnitude**. This is the
in-memory columnar **compute** ceiling on one core — it proves Drift's processing
is not the bottleneck; the `map[string]any` format was. Real ingestion (JSON
decode, broker, network) becomes the new cap, which is what parallel partition
reads ([[Parallel Source]]) and a future binary decode address.

Honest scope: Int64/Float64 `Map`/`Filter` only; aggregations/windows/joins stay on
the row path (see [[Vectorized Fast-Lane]]).

```bash
go test ./tests/bench/ -run VectorVsRow -v -count=1
```

### End-to-end with binary decode (`cmd/e2ebench`)

The fairer test: data arrives **as frames** that are **decoded in the hot path**
(decode counts), like real ingestion. Same `Filter(even)+Map(+1)` over 5M records,
`GOMAXPROCS=8`:

| config | rows/sec | vs JSON |
|---|---:|---:|
| JSON + row (`map[string]any`) | 1.20 M/s | 1.00× |
| binary + vectorized | 366 M/s | ~306× |
| parallel(8) binary + vectorized | 433 M/s | ~361× |

→ **JSON decode + `map[string]any` is the wall.** A binary columnar codec makes
decode negligible (raw byte reads, no parsing), so decode and compute overlap and
the engine is no longer the bottleneck. The parallel binary source adds ingestion
headroom; the single vectorized stage goroutine then caps it (~430 M/s) — a
parallelised stage would push further.

Honest framing: these are **in-process** frames (no network). Over a real broker,
network/disk bandwidth becomes the cap (38 MB for 5M records ⇒ ~430 M/s needs
~3 GB/s of wire) — i.e. the bottleneck moves to infrastructure, which is exactly
where you want it. The result proves Drift's decode+compute is not the limit.

```bash
go run ./cmd/e2ebench
```

---

## Resource profiles — does "beast mode" do anything?

The `Dedicated` (beast) SDK profile bundles a bigger batch (512 vs 64), bigger
buffers, and — when it `OwnsProcess()` — `GOMAXPROCS`/`GOGC`. Measured on an Apple
M-series (8 logical cores), single representative run (`tests/bench/beast_test.go`):

**Linear pipeline** (5M records, map, `GOMAXPROCS=1` — isolates batch/GC):

| config | rec/sec | vs default |
|---|---:|---:|
| default (batch 64, GOGC 100) | 1.14 M/s | 1.00× |
| beast (batch 512, GOGC 100) | 1.49 M/s | 1.30× |
| beast + GOGC 200 | 1.63 M/s | **1.43×** |

→ A real, modest win from batching + GC. Cores do **not** help a linear pipeline
(the single source goroutine is the ceiling), so the profile pins the gain to
batch/buffer/GC there.

**Compute-bound stage** (2M records, ~µs CPU per record, `pipeline.Parallel`):

| GOMAXPROCS / shards | rec/sec | vs 1 core |
|---|---:|---:|
| 1 | 155 k/s | 1.00× |
| 2 | 289 k/s | 1.86× |
| 4 | 396 k/s | 2.55× |
| 8 | 450 k/s | **2.90×** |

→ When the stage is genuinely CPU-bound and wrapped in `pipeline.Parallel`, owning
the node and giving it cores scales throughput — ~1.9× at 2 cores, tapering to
~2.9× at 8. It is **sublinear**: the single source + fan-out/gather is a serial
fraction (Amdahl), which is exactly the ceiling the parallel-ingestion work would
raise. Beast mode helps; it is not magic.

Caveats: single noisy run; M-series laptop; take medians before quoting.

```bash
go test ./tests/bench/ -run BeastMode -v -count=1
```

---

## Reproduce

```bash
# readable throughput + latency table
go test ./tests/nexmark/ -run Throughput -v -count=1

# pin cores for a per-core number
GOMAXPROCS=1 go test ./tests/nexmark/ -run Throughput -v -count=1

# Go benchmarks (the MB/s column reads as events/sec)
go test ./tests/nexmark/ -bench Nexmark -benchmem
```

Harness: `tests/nexmark/` — `events.go` (Bid generator), `queries.go` (q0/q1/q2),
`nexmark_test.go` (runner + benchmarks).
