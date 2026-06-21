---
component: operators
status: stable
package: pkg/operator
tested: true
---

# Operators

All operators live in `pkg/operator` and implement `core.Operator`. See [[Core Abstractions#Operator]] for the interface contract.

---

## Map

```go
NewMap(fn MapFunc) *Map
type MapFunc func(core.Record) (core.Record, error)
```

- 1-to-1 transform
- If `fn` returns an error, `Process` halts the batch and returns the error
- `OnSchemaChange` stores schema for use in `fn` (pass via closure)

**Invariant:** output record count == input record count

---

## Filter

```go
NewFilter(pred PredicateFunc) *Filter
type PredicateFunc func(core.Record) bool
```

- Passes only records where `pred` returns `true`
- Zero allocations when all records pass (reuses input slice)
- **Idempotent**: `Filter(Filter(x)) == Filter(x)`

---

## FlatMap

```go
NewFlatMap(fn FlatMapFunc) *FlatMap
type FlatMapFunc func(core.Record) ([]core.Record, error)
```

- 1-to-N (or 1-to-0) transform
- `return nil, nil` drops the record — equivalent to filter-out
- Output count is not bounded

---

## SchemaAdapter

```go
NewSchemaAdapter(initial core.Schema, aliases AliasMap) *SchemaAdapter
type AliasMap map[string]string // oldName → newName
```

- Normalises records to the current schema (full rules in [[Schema Evolution#SchemaAdapter]])
- Thread-safe: `OnSchemaChange` holds write-lock; `Process` holds read-lock
- Must be placed **before** any operator that assumes a specific schema shape

---

## TumblingWindow

```go
NewTumblingWindow(size int, fn AggregateFunc) (*TumblingWindow, error)
type AggregateFunc func(window []core.Record) (core.Record, error)
```

- Collects exactly `size` records, then emits one aggregate record
- Implements `core.Flusher` — pipeline calls `Flush()` on source close to emit partial window
- `size` must be ≥ 1

**State:** internal buffer of up to `size` records. Protected by mutex.

---

## SlidingWindow

```go
NewSlidingWindow(size, step int, fn AggregateFunc) (*SlidingWindow, error)
```

- Every `step` records: emits aggregate of the last `size` records
- Windows overlap when `step < size`; equivalent to tumbling when `step == size`
- Implements `core.Flusher` — emits partial step on source close
- Constraints: `size ≥ step ≥ 1`

---

## TimestampAssigner

```go
NewTimestampAssigner(fn TimestampFunc) *TimestampAssigner
type TimestampFunc func(core.Record) time.Time
```

- Sets `EventTime` on each record from `fn` (e.g. extract a `"ts"` field from the payload)
- 1-to-1, never drops records; output count == input count
- Place **before** any event-time window so downstream operators have a populated `EventTime`
- Mutex **not required**: does not read schema in `Process`
- Does not implement `core.Flusher` or `core.Snapshottable` — stateless

---

## EventTimeWindow

```go
NewEventTimeWindow(size, allowedLateness time.Duration, fn AggregateFunc) (*EventTimeWindow, error)
```

- **Event-time tumbling window**: each record falls into the window `[start, start+size)`
  where `start = EventTime` truncated to a `size` boundary (aligned from the zero time)
- Fires a window's aggregate once the [[Core Abstractions#Watermarks (event time)|watermark]]
  (`maxEventTimeSeen − allowedLateness`) reaches `start+size`
- **Late records** (window already fired: `windowEnd ≤ watermark`) are dropped and counted; `LateDropped()` exposes the count
- `Watermark()` returns the current watermark (zero time if no records seen yet) — useful for metrics/tests
- Implements `core.Flusher` — `Flush()` advances the watermark to +∞ and emits all remaining windows in ascending start order
- **Snapshottable**: persists open windows + `maxEventTimeSeen` + late count across restarts
- Fired windows are emitted in ascending window-start order (deterministic)
- Constraints: `size ≥ 1ns`, `allowedLateness ≥ 0`
- Mutex **not required**: window state is touched only from `Process`/`Flush`/`Snapshot` (pipeline guarantees no concurrent calls); `schema` is stored by `OnSchemaChange` but never read in `Process`

**Invariant:** every non-late record contributes to exactly one window; each window fires at most once.

**Contrast with [[#TumblingWindow]]:** TumblingWindow is *count*-based (every `size` records); EventTimeWindow is *time*-based (wall-clock `size` of event time, gated by the watermark).

---

## SessionWindow

```go
NewSessionWindow(gap time.Duration, keyFn KeyFunc, fn AggregateFunc) (*SessionWindow, error)
type KeyFunc func(core.Record) string // shared with Deduplicate
```

- **Event-time session window**: groups records per key into sessions of activity
  separated by gaps of inactivity. A record extends a session if its `EventTime`
  is within `gap` of the session's span `[min, max]`; otherwise it starts a new session.
- **Keyed**: `keyFn` partitions records into independent sessions. For a single
  global session, return a constant key.
- **Gap = inactivity timeout = lateness tolerance.** A session fires once the
  [[Core Abstractions#Watermarks (event time)|watermark]] (`maxEventTimeSeen`)
  reaches `sessionMax + gap` — i.e. an event arrived at least `gap` beyond the
  session's last event, so nothing more can extend it. No separate lateness param.
- **Merging**: an out-of-order record that bridges two open sessions merges them
  into one (sessions are kept sorted by start and merged whenever they come within `gap`).
- **Late records** (would form a session already past the fire watermark:
  `EventTime + gap ≤ firedUpTo`, and don't merge into an open session) are dropped
  and counted; `LateDropped()` exposes the count.
- `Watermark()` returns `maxEventTimeSeen` (zero if none seen).
- Implements `core.Flusher` — `Flush()` fires all remaining open sessions in
  ascending start order.
- **Snapshottable**: persists open sessions + `maxSeen` + `firedUpTo` + late count.
- Constraints: `gap ≥ 1ns`, `keyFn` non-nil.
- Mutex **not required**: session state touched only from `Process`/`Flush`/`Snapshot`; `schema` stored by `OnSchemaChange`, never read in `Process`.

**Invariant:** each non-late record belongs to exactly one fired session; sessions for the same key never overlap after merging.

**Contrast:** [[#TumblingWindow]] is count-based, [[#EventTimeWindow]] is fixed-size event-time, **SessionWindow** is dynamic event-time bounded by inactivity gaps.

---

## Adding a new operator

Follow `skills/add-operator.md`. Required checklist:
- [ ] Spec in `specs/operators.md` (and here in this note)
- [ ] Implementation in `pkg/operator/<name>.go`
- [ ] `OnSchemaChange` uses `sync.RWMutex` if operator reads schema in `Process`
- [ ] Unit tests: happy path, error path, `OnSchemaChange` concurrent with `Process`
- [ ] If stateful: implements `core.Flusher` + flush test

---

## Deduplicate

```go
NewDeduplicate(keyFn KeyFunc, window time.Duration) *Deduplicate
type KeyFunc func(core.Record) string
```

- Drops records whose key (from `keyFn`) was seen within the last `window` duration
- `window=0`: no deduplication — all records pass through
- Key-to-timestamp map is evicted lazily at the start of each `Process` call
- **Snapshottable**: persists the seen-map across pipeline restarts; expired entries pruned on `Restore`
- Does **not** implement `core.Flusher` — no output-pending state
- Mutex **not required**: `seen` map is accessed only from `Process`; `schema` is stored by `OnSchemaChange` but never read inside `Process`

**Invariant:** a record with key K is passed at most once per `window` duration

---

## Merge

```go
NewMerge(extra <-chan []core.Record) *Merge
```

- Passes through all records from the pipeline primary (`in`) and non-blocking drains `extra` on each `Process` call
- `extra` must be provided by the caller; `Merge` does not own or close it
- **Limitation (linear executor):** if the primary stream produces no records, `extra` records are not drained until the next primary batch arrives
- Does **not** implement `core.Flusher` or `core.Snapshottable` — state is held by the channels themselves

---

## Split

```go
NewSplit(n int, routerFn RouterFunc, bufSize int) (*Split, error)
type RouterFunc func(core.Record) int
func (s *Split) Outputs() []<-chan core.Record
func (s *Split) Close()
```

- Routes each record to one of N output routes via `routerFn`
- Route 0 is returned from `Process` (pipeline-integrated, gets metrics and backpressure)
- Routes 1..n-1 are written to side channels from `Outputs()` (blocking write — natural backpressure)
- Out-of-range values from `routerFn` are clamped to route 0
- `n < 2` returns an error from `NewSplit`
- Caller must drain all side channels and call `Close()` after the pipeline stops
- Does **not** implement `core.Flusher` or `core.Snapshottable`

---

## Group-by & top-N (Nexmark operators)

Two keyed, windowed operators added for [[Nexmark Coverage]]. Both are
`Flusher` + `Snapshottable` and key-partitionable under `pipeline.Parallel`.

- **`KeyedCountWindow(keyFn, size, KeyedAggregateFunc)`** — group-by over fixed-
  count windows: buffers records per key, fires the aggregate once a key reaches
  `size`, resets that key. `KeyedAggregateFunc(key, window)` returns one record;
  `CountAgg(field)` is the built-in count. Flush emits partial windows in key
  order. Powers q5/q12/q15/q16/q17, and q18 (a last-row aggregate = keep-latest).
- **`TopN(keyFn, ValueFunc, n, size)`** — top-n by value within a count window,
  per key (`keyFn=nil` ⇒ global). Emits the top n records each tagged with a
  1-based `rank`. Powers q19 (per-auction) and q5 (global hottest).

These are **count-window** operators (a faithful throughput proxy for Nexmark's
time windows); event-time variants can build on the same shape.

- **`Join(leftType, leftKey, rightType, rightKey, maxPerKey, JoinFunc)`** — a
  streaming hash equi-join over a **single mixed stream**: records are dispatched
  by `SchemaID`, buffered per join key (bounded to `maxPerKey` per side), and each
  arrival is matched against the opposite side. Inner-join semantics,
  `Snapshottable`. Powers q3/q8/q20 and the join half of q4/q6/q9 (composed with
  `KeyedCountWindow`). This is how Drift joins without a second source — the
  Nexmark generator interleaves event types in one stream.

---

## Intra-stage parallelism (`pipeline.Parallel`)

Any stage can shard its work across N goroutines via the YAML `parallelism: N`
field, wired by `pipeline.Parallel(ops, keyFn)` — N fresh operator instances run
concurrently, the executor unchanged.

| Operator class | Sharding | Notes |
|---|---|---|
| Stateless (map-set, map-rename, filter, timestamp) | round-robin (`keyFn=nil`) | safe; order not preserved |
| Keyed stateful (dedup, session) | by key hash (`fieldKey(key)`) | same key → same shard → correct; Flush/Snapshot fan over shards |
| Global windows (tumbling, eventwindow) | **not allowed** | no partition key — `job.Load` rejects `parallelism > 1` |
| `ref:` ops | **not allowed** | keying unknown — rejected |

`Parallel` preserves the inner ops' `Flusher`/`Snapshottable` interfaces (drains
and snapshots every shard). Catalog flags each block's `Parallelizable`.

**Tests:** `TestParallel_*` (pkg/pipeline), `TestBuildStageOp_*` (pkg/job).

---

## See also

- [[Core Abstractions#Flusher]]
- [[Schema Evolution#SchemaAdapter]]
- [[Overview#Parallelism]]
- [[Testing#Operator tests]]
