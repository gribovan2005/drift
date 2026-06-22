---
component: resource-profiles
status: stable
package: pkg/pipeline, sdk
tested: true
---

# Resource Profiles (Sidecar & Dedicated)

The same Drift binary runs in two very different deployment shapes; profiles bundle
the resource knobs for each so callers don't hand-tune:

- **Sidecar** — runs next to a latency-sensitive service on the same node. A polite
  neighbour: small footprint, low per-record latency, tolerates lag. "Eat little."
- **Dedicated** (a.k.a. Beast) — owns the node as a standalone consumer. Pushes
  throughput. "Eat the node."

```go
// embedded next to a host service — local knobs only, never touches host runtime
sdk.New(sdk.WithProfile(sdk.Sidecar)).From(src).Map(fn).To(sink).Run(ctx)

// standalone consumer that owns its process — may tune GOMAXPROCS/GOGC
sdk.New(sdk.WithProfile(sdk.Dedicated.OwnsProcess())).From(src)...
```

---

## Engine knobs (`pkg/pipeline`)

Three composable, **side-effect-free** Options (safe in any deployment):

```go
func WithBatchSize(n int) Option       // stage batch size (default 64)
func WithChannelBuffer(n int) Option   // global channel buffer (default 256; per-stage Stage.BufSize still wins)
func WithMaxLinger(d time.Duration) Option // time-based partial-batch flush (default 0 = off)
```

### Max-linger semantics (the latency enabler)

`runStage` flushes a batch when it reaches `batchSize` **or** the input channel
closes — there is **no** time-based flush by default. So a small batch alone does
NOT lower latency under sparse input (records sit in the partial batch until enough
arrive). `WithMaxLinger(d>0)` adds a ticker to the stage loop: a partial batch is
flushed every `d`. `d<=0` ⇒ no ticker created ⇒ zero overhead, behaviour identical
to today. This is what makes Sidecar's low-latency promise real.

---

## Profiles (`sdk`)

```go
type Profile struct {
    BatchSize     int           // → WithBatchSize
    ChannelBuffer int           // → WithChannelBuffer
    MaxLinger     time.Duration // → WithMaxLinger
    GOMAXPROCS    int           // process-global; 0 = preset default (NumCPU-derived)
    GCPercent     int           // process-global (GOGC)
    MemLimit      int64         // process-global (GOMEMLIMIT); 0 = leave unset
    // ownsProcess (unexported) — gates the process-global knobs; set via OwnsProcess()
}

var Sidecar   = Profile{BatchSize: 16,  ChannelBuffer: 32,   MaxLinger: 5*time.Millisecond, GCPercent: 50}
var Dedicated = Profile{BatchSize: 512, ChannelBuffer: 1024, MaxLinger: 0,                  GCPercent: 200}

func (p Profile) OwnsProcess() Profile           // copy with process-global knobs enabled
func WithProfile(p Profile) Option               // SDK Option
```

Resolved `GOMAXPROCS` (when owned, field left 0): Sidecar → `max(1, NumCPU/4)`,
Dedicated → `NumCPU`. `MemLimit` is **never** preset (a preset can't know the
node's budget); set it explicitly:

```go
p := sdk.Sidecar; p.MemLimit = 512 << 20
sdk.New(sdk.WithProfile(p.OwnsProcess()))
```

### Embedded vs OwnsProcess (the safety split)

- **Default `WithProfile(p)`** applies only the LOCAL engine knobs (batch / buffer /
  linger). Safe when embedded in a host process — it never re-tunes the host.
- **`WithProfile(p.OwnsProcess())`** additionally applies the process-global knobs
  (`runtime.GOMAXPROCS`, `debug.SetGCPercent`, and `debug.SetMemoryLimit` when
  `MemLimit>0`). Use ONLY when Drift owns the process (a standalone/dedicated
  consumer), never inside someone else's service.

### Overrides

Granular options compose and **later wins**, so a preset can be tweaked inline:

```go
sdk.New(sdk.WithProfile(sdk.Dedicated), sdk.WithBatchSize(256)) // batch 256, rest from Dedicated
```

---

## Scope (honest)

Profiles tune **knobs only**. They do **not**:

- raise the single-source ingestion ceiling or change the `map[string]any` record
  format (separate, larger future work — see [[Benchmarks]]);
- auto-parallelise stages — stage data-parallelism stays manual via
  `Apply(pipeline.Parallel(...))` or job `StageSpec.Parallelism`.

### YAML / runner path

A job spec may set a top-level `profile: sidecar|dedicated` field; `job.Load`
validates it and `Built.Pipeline` prepends the profile's **local** options
(batch/buffer/linger) via `sdk.ProfileByName(...).PipelineOptions()`. Process-global
knobs (GOMAXPROCS/GOGC) are **not** applied from a job — one job must not re-tune
the whole `drift serve` process; use the SDK with `OwnsProcess()` for a dedicated
single-job binary instead.

```yaml
name: payments
profile: sidecar          # tunes batch/buffer/linger; engine defaults if omitted
source: { type: kafka, ... }
stages: [ ... ]
sink: { type: ... }
```

---

## Invariants

1. **Local knobs are pure config** — no process-global side effects from
   `WithBatchSize`/`WithChannelBuffer`/`WithMaxLinger` or default `WithProfile`.
2. **Process-global only via `OwnsProcess()`** — an embedded library must never
   silently change the host's `GOMAXPROCS`/`GOGC`/memory limit.
3. **`MaxLinger<=0` is a no-op** — no ticker, identical data-path behaviour and cost.
4. **Per-stage `Stage.BufSize` overrides the global `WithChannelBuffer`.**
5. Runtime-knob resolution is a **pure function** (`runtimeSettings`) so it is
   testable without mutating global state.

---

## Required tests (no mocks; real in-process pipelines; `-race` green)

- `pkg/pipeline`: `WithBatchSize`/`WithChannelBuffer` reach the pipeline (correct
  output; tiny buffer completes without deadlock); **`MaxLinger_FlushesPartialBatch`**
  (sparse source emits k<batchSize then blocks → sink gets all k within ~linger;
  negative control without linger → not flushed before cancel); `Linger_Zero` keeps
  size-based flush.
- `sdk`: `Profile_RuntimeSettings` (pure values for Sidecar/Dedicated, no mutation);
  `WithProfile_AppliesLocalKnobs` (sparse source flushes via Sidecar linger);
  `WithProfile_NoOwnsProcess_LeavesRuntime` (`GOMAXPROCS` unchanged);
  `WithProfile_OwnsProcess_AppliesRuntime` (save/restore globals; not parallel);
  `GranularOverride_WinsOverProfile`.

---

## See also

- [[SDK]] — `WithProfile` lives on the SDK facade
- [[Core Abstractions]] — Operator/Flusher batching the knobs tune
- [[Benchmarks]] — why the single-source/`map[string]any` ceiling is separate work
