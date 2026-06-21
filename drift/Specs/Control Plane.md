---
component: control-plane
status: implemented
package: pkg/runner
file: pkg/runner/runner.go
tested: true
---

# Control Plane

Runtime management of pipelines: a **job store** (YAML files in a folder) plus a
**runner** that builds, starts, stops, and reports the status of the *currently
running* pipeline. This is what lets the [[Web UI & Builder]] author a pipeline,
save it, and run it — without restarting the process.

Drift stays single-process and the DAG stays immutable (see [[Overview]]). The
control plane never reshapes a running pipeline: to "change" a pipeline it builds
a **fresh** one via `job.Load` and swaps a pointer. This also sidesteps the fact
that a `pipeline.Pipeline` cannot be re-run (its source channel exhausts) — every
run is a new pipeline with fresh operator state.

---

## Store (`pkg/runner/store.go`)

Filesystem CRUD over a directory of `*.yaml` job files. The wire/UI exchanges
`job.Spec` as JSON; the store persists it as YAML via `job.Marshal`.

```go
type JobInfo struct {
    Name    string
    ModTime time.Time
    Valid   bool   // does it pass job.Load?
    Error   string // first validation error, if any
}

func NewStore(dir string) (*Store, error)
func (s *Store) List() ([]JobInfo, error)
func (s *Store) Load(name string) (job.Spec, error)
func (s *Store) Save(spec job.Spec) error           // validate-before-save
func (s *Store) Delete(name string) error
func (s *Store) Duplicate(name, newName string) error
```

- **Validate-before-save**: `Save` marshals the spec and runs `job.Load` as a dry
  run; the file is written only if it loads. Invalid specs never reach disk.
- **Path-traversal guard**: job names must match `^[A-Za-z0-9_-]+$`. Any other
  name is rejected before touching the filesystem. The filename is `<name>.yaml`.
- A file that fails to parse still appears in `List()` with `Valid:false` and the
  error, so the UI can surface broken jobs rather than hiding them.

---

## Runner (`pkg/runner/runner.go`)

Manages **any number of concurrently running pipelines**, keyed by job name,
behind one `sync.RWMutex` (a `map[string]*run`). Each run is an independent
pipeline built per `Start` via `job.Load` (fresh operator state).

```go
type JobStatus struct { Name string; StartedAt time.Time; Uptime time.Duration }
type Status struct {
    Running []JobStatus
    Errors  map[string]string // name → last error from a finished run
}

func New(store *Store, opts ...Option) *Runner
func (r *Runner) Start(name string) error              // error only if that job is already running
func (r *Runner) Stop(name string) error               // cancels + waits, then removes
func (r *Runner) StopAll()                              // shutdown
func (r *Runner) Status() Status                        // all running jobs + recent errors
func (r *Runner) Current(name string) *pipeline.Pipeline // nil if not running ("" → sole run)
func (r *Runner) Sample(name, stage string) []core.Record
func (r *Runner) Store() *Store
```

- **Start**: loads the YAML, `job.Load` → `Built.Pipeline()` (+ a `pipeline.Tap`),
  per-run `context.WithCancel`, launches `go p.Run(ctx)`, registers the run under
  its name. Errors only if that **same** job is already running.
- **Stop(name)**: cancels that run, waits up to ~5s for `Run` to return, removes
  it. On timeout it detaches anyway. Stopping a non-running job is a no-op.
- **Current(name)**: read under `RLock`; the monitoring endpoints call it every
  SSE tick with the `?job=` they're watching, so each dashboard follows its own
  live pipeline. `""` returns the sole run when exactly one is running.
- **Uptime** (per `JobStatus`) is the source of the UI's uptime read-out.

### Concurrency

Pipelines are immutable after build; `Snapshot`/`Graph`/`IsReady`/`Tap.Sample`
are goroutine-safe. The only shared mutable state is the `runs` map, fully guarded
by the `RWMutex`. `TestRunner_ConcurrentCurrentDuringSwap` spams `Current`/`Status`
while looping `Start`/`Stop` on two jobs under `-race`.

---

## Limitations

- **Built-ins only for stored jobs.** `ref:` operators/sources are package-global
  singletons that retain state and whose source channel can exhaust — they don't
  survive a rebuild-per-run cleanly. The builder offers built-ins only; `ref:` is
  still usable from `drift run`.

---

## Required tests

| Test | Proves |
|---|---|
| `TestRunner_StartStopStatus` | start → in Running; stop → empty |
| `TestRunner_MultipleConcurrent` | two jobs run at once; stopping one leaves the other |
| `TestRunner_RebuildPerRun` | two runs yield distinct pipelines; stateful op resets |
| `TestRunner_StartSameJobTwice` | starting an already-running job errors |
| `TestRunner_StopWhenIdle` | Stop on a non-running job is a no-op, no panic |
| `TestRunner_CurrentNilWhenIdle` | Current(name) is nil before start / after stop |
| `TestRunner_ConcurrentCurrentDuringSwap` | `-race`: Current/Status during Start/Stop loop |
| `TestStore_SaveLoadRoundTrip` | Save → Load yields the same spec |
| `TestStore_ListDelete` | List reflects writes; Delete removes |
| `TestStore_Duplicate` | Duplicate copies under a new name |
| `TestStore_ValidateBeforeSave` | an invalid spec is not written |
| `TestStore_PathTraversalRejected` | `../etc/x` style names are refused |

---

## See also

- [[Web UI & Builder]] — the HTTP surface + visual editor over this
- [[CLI & Jobs]] — `drift serve`, `job.Marshal`, `job.Catalog`
- [[Overview]] — single-process, immutable-DAG context
