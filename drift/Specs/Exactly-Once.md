---
component: exactly-once
status: implemented
package: pkg/wal
file: pkg/wal/log.go
tested: true
---

# Exactly-Once (WAL + idempotent sink)

The last item of [[Overview|Path A]]. Goal: a pipeline survives a crash/restart
**without losing records and without writing duplicates** to its sink — *effectively
once* end-to-end on a single process.

Two cooperating pieces, both built on a durable **Write-Ahead Log**:

1. **WAL source** — appends every record to the log (fsync) **before** emitting it
   into the pipeline, and **replays** any un-committed log entries on restart.
   This is the "no loss" half: a record that entered the pipeline but whose effect
   never reached the sink is durably recoverable.
2. **Idempotent sink** — wraps the real sink with a **durable seen-set** keyed by a
   stable per-record *delivery key*. A replayed record carries the same key it had
   on the first run, so the wrapper skips it. This is the "no duplicates" half. After
   a record is durably written, the sink **acks** it back to the WAL, which advances
   its commit watermark — *offsets are committed only after the sink acknowledges*.

A shared `wal.Coordinator` owns the log and hands out the matched source/sink
wrappers, so the sink's ack reaches the same log the source reads from. Single
process, so this is an in-memory reference, not an RPC.

---

## Delivery key

Exactly-once needs a key that is **identical across replays** so the sink can
recognise a duplicate. One field on `core.Record`:

```go
type Record struct {
    // ...
    DeliveryKey string // stable across replays; set by the WAL source, read by
                       // the idempotent sink. Empty when exactly-once is off.
}
```

- Empty when EOS is **off** — zero overhead, no behaviour change (mirrors
  `ID`/`Parents` for [[Lineage]]).
- The WAL source sets it to `wal:<LSN>` where LSN is the log sequence number it
  assigned on append. Deterministic: the same source record replayed from the log
  re-emits with the same LSN, hence the same key.

---

## WAL (`pkg/wal`)

A dependency-free, append-only log on disk.

```go
type Entry struct {
    LSN  uint64
    Data []byte
}

func Open(dir string) (*Log, error)            // creates dir; recovers next LSN + commit watermark
func (l *Log) Append(data []byte) (uint64, error) // fsync'd append; returns assigned LSN
func (l *Log) Commit(lsn uint64) error          // persist "durably processed through lsn"
func (l *Log) Committed() uint64                 // current commit watermark
func (l *Log) Uncommitted() ([]Entry, error)    // entries with LSN > watermark, in order
func (l *Log) Close() error
```

- **Format**: one segment file `log.wal`, each record framed as
  `[u32 length][u64 lsn][length bytes payload]`. Appends are followed by `fsync`.
- **Commit watermark**: a tiny `commit` file written atomically (`.tmp` + rename),
  holding the highest contiguously-processed LSN. Survives crashes; a half-written
  watermark falls back to the previous one.
- **Recovery**: `Open` scans `log.wal` to find the max LSN (next = max+1) and reads
  the commit watermark. A torn trailing frame (partial length/payload from a crash
  mid-append) is detected and ignored — the log is treated as ending at the last
  intact frame.
- **Concurrency**: guarded by a mutex; `Append`/`Commit`/`Uncommitted` are safe to
  call from different goroutines (source goroutine appends, sink goroutine commits).

---

## Coordinator (`pkg/wal`)

```go
func NewCoordinator(log *Log, seen checkpoint.Store) *Coordinator
func (c *Coordinator) Source(inner core.Source) core.Source
func (c *Coordinator) Sink(inner core.Sink) core.Sink
```

- `Source(inner)`: on `Read`, first replays `log.Uncommitted()` (records that
  crashed before their effect was durably committed), then drains `inner`. Every
  emitted record is `Append`-ed and stamped with `DeliveryKey = wal:<LSN>`.
- `Sink(inner)`: maintains a durable seen-set of delivery keys via the
  `checkpoint.Store`. For each record: if its key is already in the set → skip
  (duplicate); otherwise write it to `inner`, persist the key, then `log.Commit`
  the contiguous LSN prefix that has been durably written. The seen-set is the
  source of truth for the watermark, so it is recomputed correctly on restart.

---

## Guarantee & limits

| Pipeline shape | Guarantee |
|---|---|
| Stateless / passthrough (map, filter, flatmap, dedup) | **exactly-once** end-to-end across crash + replay — the source LSN flows unchanged to the sink, so replays dedup perfectly |
| Aggregating (windows) | **at-least-once** at the sink unless the aggregate carries a deterministic `DeliveryKey` — an aggregate is derived from many source records, so no single source LSN identifies it |

This is the honest single-process boundary: true exactly-once *through*
aggregation needs atomic {source-offset + operator-state} checkpoints
(Chandy-Lamport barriers), which the fixed-DAG executor does not inject. The WAL
gives no-loss everywhere and no-duplicates for the passthrough majority; windows
keep their existing [[Operators|checkpoint]] state via `Snapshottable`.

---

## Required tests

| Test | Proves |
|---|---|
| `TestLog_AppendAssignsMonotonicLSN` | LSNs start at 1 and increment |
| `TestLog_RecoverNextLSN` | reopen continues numbering past the max |
| `TestLog_CommitWatermarkPersists` | commit survives reopen |
| `TestLog_UncommittedAfterCommit` | only LSN > watermark returned, in order |
| `TestLog_TornFrameIgnored` | a truncated trailing frame is dropped on recover |
| `TestLog_Concurrent` | `-race`: parallel Append + Commit |
| `TestCoordinator_NoDuplicatesOnReplay` | replayed records are skipped by the sink |
| `TestCoordinator_NoLossOnCrash` | un-committed records are replayed after restart |
| `TestCoordinator_CommitAdvancesOnAck` | watermark advances only after sink writes |
| `TestPipeline_ExactlyOnce_CrashReplay` | end-to-end: crash mid-run, restart, sink has each record exactly once |

---

## See also

- [[Overview]] — Path A roadmap
- [[Sources & Sinks]] — the wrapped Source/Sink implementations
- [[Core Abstractions]] — the `Record` type and `DeliveryKey`
- [[Lineage]] — the other `core.Record` add-on field pattern
