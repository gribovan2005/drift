---
component: vectorized-fastlane
status: stable
package: pkg/core (Batch), pkg/vector
tested: true
---

# Vectorized Fast-Lane (columnar)

`map[string]any` is the throughput wall: every value is boxed (heap alloc) and GC
must scan a huge live heap (measured — parallel pipelines plateau ~3–4 M/s on it).
The fast-lane processes data **columnar** — typed slices, no map, no boxing, tight
per-column loops — the model ClickHouse/Arroyo use. It is **additive**: the
existing row (`map[string]any`) engine is untouched; windows/joins/rich operators
stay on the row path.

## Integration: chunk-records (runs in the normal pipeline)

The fast-lane needs **no new executor**. A columnar block of N rows travels through
`pipeline.Pipeline` as a single **chunk-record**: a `core.Record` whose `Chunk`
field holds a `*core.Batch`. Because a chunk *is* a `core.Record`, vectorized
operators are ordinary `core.Operator`s and the existing channels/DAG/runStage/
metrics are unchanged. Batches pass between vectorized stages **without** row
materialisation — conversion to rows happens only at a row-operator boundary or a
row sink (`vector.ToRows`). It composes with the SDK directly (every constructor
returns a `core.*` type):

```go
sdk.New().
    From(vector.MemSource(batches)).
    Apply(vector.FilterInt64("v", func(x int64) bool { return x%2 == 0 })).
    Apply(vector.MapInt64("v", func(x int64) int64 { return x + 1 })).
    To(vector.Discard()).
    Run(ctx)
```

---

## Types (`pkg/core/batch.go`)

`pkg/core` keeps its no-import rule — `Batch` uses only stdlib + the existing
`Schema`.

```go
type ColumnKind uint8
const ( KindInt64 ColumnKind = iota; KindFloat64; KindString; KindBool )

type Column struct {
    Kind ColumnKind
    I64  []int64
    F64  []float64
    Str  []string
    B    []bool
}

type Batch struct {
    Schema Schema   // field names + types
    Len    int      // valid rows (≤ cap of the column slices)
    Cols   []Column // parallel to Schema.Fields
}

func (b *Batch) Int64(field string) []int64      // nil if missing/wrong kind
func (b *Batch) Float64(field string) []float64
func (b *Batch) copyRow(dst, src int)            // align all columns (compaction)
func (b *Batch) truncate(n int)                  // shrink all columns to n
```

And one additive field on `Record` (nil for normal row records, `omitempty` so the
JSON wire format is unchanged):

```go
type Record struct { ... ; Chunk *Batch `json:"chunk,omitempty"` }
```

## Operators (`pkg/vector`, implements `core.Operator`)

```go
// Map / Filter (Int64, Float64, String, Bool)
func MapInt64(field string, fn func(int64) int64) core.Operator
func FilterInt64(field string, pred func(int64) bool) core.Operator
func MapFloat64(field string, fn func(float64) float64) core.Operator
func FilterFloat64(field string, pred func(float64) bool) core.Operator
func MapString(field string, fn func(string) string) core.Operator
func FilterString(field string, pred func(string) bool) core.Operator
func FilterBool(field string, pred func(bool) bool) core.Operator

// Global aggregates (Flusher: accumulate over all chunks, emit one row Record on flush)
func SumInt64(field, out string) core.Operator
func SumFloat64(field, out string) core.Operator
func MaxInt64(field, out string) core.Operator
func CountRows(out string) core.Operator

// Parallel: run n copies of a stateless op across n goroutines (round-robin chunks)
func Parallel(n int, mk func() core.Operator) core.Operator
```

- Map runs a tight in-place loop over one column across each chunk in the batch.
- Filter computes the keep set and **compacts all columns** in place (via
  `CopyRow`) then `Truncate`s, updating `Batch.Len`. No per-row alloc, no boxing.
- `OnSchemaChange` is a no-op — the schema travels inside each `Batch`.
- Chunks with `Chunk == nil` (stray row records) pass through untouched.
- **Aggregates** return nothing during `Process` and emit a single **row** Record
  (`{out: result}`) on `Flush` — the same pattern as the row windows. The scalar
  result leaves the columnar world, so it goes to any normal sink. They are
  **single-stage** — do not wrap an aggregate in `Parallel` (you'd get per-shard
  partials); only stateless Map/Filter are parallelisable.
- **`Parallel(n, mk)`** wraps `pipeline.Parallel`: it round-robins whole chunk-
  records across `n` fresh operators on `n` goroutines, so a CPU-heavy vectorized
  stage scales with cores (measured ~5.8× at 8 cores; see [[Benchmarks]]). Each
  chunk goes to exactly one shard, so the in-place mutation stays safe.

## Source / sink / bridge (`pkg/vector`)

```go
func MemSource(batches []*core.Batch) core.Source   // emits one chunk-record per batch
func GenInt64(field string, nBatches, rows int, fill func(i int) int64) []*core.Batch // bench/test helper
func Collect() *Collector                            // keeps chunks; .Rows() / .Batches()
func Discard() core.Sink                              // drains chunk-records
func ToRows() core.Operator                          // expand a chunk → row Records (handoff to row path/sinks)

// Binary columnar codec + wire source (decode counts toward throughput):
func EncodeBatch(b *core.Batch) ([]byte, error)      // binary columnar frame (Int64/Float64)
func DecodeBatch(data []byte) (*core.Batch, error)   // hand-rolled fast decode
func BinSource(frames [][]byte) core.Source          // decode frames → chunk-records (model a binary topic)
func KafkaColumnarSource(brokers []string, topic string, partition int) core.Source // decode a Kafka partition's binary frames
```

### Binary codec (vs JSON)

`EncodeBatch`/`DecodeBatch` are a compact column-oriented binary format: a small
header (field names + kinds + row count) then raw little-endian column bytes.
Decode is a few tight loops over raw bytes — no parsing, no per-value alloc, no
boxing — so unlike JSON the decode cost is negligible next to compute. `BinSource`
decodes in the read path, modelling a Kafka topic of binary-columnar messages; wrap
N `BinSource`s in `source.NewParallel` to read partitions concurrently. End-to-end
(decode in the hot path, 5M records): JSON+row `~1.2 M/s` vs binary+vectorized
`~360 M/s`, parallel binary+vec `~430 M/s` — see [[Benchmarks]] and `cmd/e2ebench`.

---

## Scope (honest)

- **In scope:** `Int64`/`Float64`/`String`/`Bool` columns; `Map`/`Filter`; global
  aggregates (`Sum`/`Max`/`Count`); per-stage parallelism (`Parallel`); columnar mem
  source + collect/discard sinks + a row bridge. Covers the stateless transform hot
  path plus simple `SELECT sum/count/max ... WHERE ...`.
- **Out of scope (stay on the row path):** windowed/keyed aggregations, joins,
  schema evolution, WAL. The binary **codec is Int64/Float64 only** — String/Bool
  columns can't yet cross the binary wire (`EncodeBatch` errors on them). The
  fast-lane does **not** replace the engine.
- **Caveats:** vectorized Map/Filter mutate chunks in place — correct for a linear
  pipeline; a fan-out DAG sharing a chunk would need a copy (not provided this
  iteration). Chunk-records must not hit JSON/row sinks directly — use `ToRows` first.
- Pipeline metrics are **row-accurate** for chunk-records: `runStage` counts
  `Batch.Len` rows per chunk (not 1 per chunk-record), so ProcessedTotal/throughput
  reflect real rows.

---

## Required tests (no mocks; real `pipeline.New`/SDK; `-race` green)

- `pkg/core`: `Int64`/`Float64` accessors (incl. missing/wrong-kind → nil),
  `copyRow`/`truncate` keep columns aligned, `Record.Chunk` JSON omitempty (a normal
  row record marshals without a `chunk` key).
- `pkg/vector`: `MapInt64`/`MapFloat64` correctness **through `pipeline.New`**;
  `FilterInt64` compaction with a second column to prove alignment + `Len` update;
  end-to-end Filter+Map via the SDK matches the equivalent row pipeline result;
  `ToRows` expands a chunk to the right row records.
- `tests/bench`: vectorized Filter+Map over an Int64 column vs the equivalent
  `map[string]any` row pipeline — expect a large multiple (documented in
  [[Benchmarks]]).

---

## See also

- [[Core Abstractions]] — Record/Operator the chunk-record extends
- [[Parallel Source]] — parallel ingestion that feeds the fast-lane
- [[Benchmarks]] — the `map[string]any` ceiling this lifts
- [[SDK]] — the fluent builder vectorized ops plug into via `Apply`
