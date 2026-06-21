---
component: web-ui
status: implemented
package: pkg/web
file: pkg/web/server.go
tested: true
---

# Web UI & Builder

The Drift web surface has two views, both served by `pkg/web` (vanilla JS + SVG,
`go:embed`'d, no build step):

1. **Monitor** — the live dashboard (graph, per-stage cards, schema timeline, AI
   explain, DLQ). Hardened: SSE reconnect, loading/empty/error states, responsive,
   plus an **advanced-metrics drawer** per stage.
2. **Builder** — a visual DAG editor. Drag blocks from a palette, wire them
   (edges = `next`), configure params, and **save to / load from** YAML job files
   via the [[Control Plane]]. Run and stop pipelines from the UI.

The Builder appears only when the server is started with a runner
(`web.WithRunner`); `drift run --ui` and the demo stay monitor-only.

---

## Provider model

The server reads the pipeline through an interface so the monitored pipeline can
be fixed (legacy) or runner-managed (control plane):

```go
type PipelineProvider interface { Current(job string) *pipeline.Pipeline }
```

- `web.New(addr, p, reg, dbg, opts...)` wraps a fixed `*pipeline.Pipeline` in a
  `staticProvider` — **unchanged behaviour** for `cmd/demo` and `drift run --ui`.
- `web.WithRunner(r)` swaps in the `*runner.Runner` (it implements
  `PipelineProvider`) and enables the control-plane endpoints + Builder view.

When `Current()` is `nil` (idle), monitor endpoints degrade gracefully:
`/api/graph`→`[]`, `/api/events`→emits `{"state":"idle"}` each tick (keeps SSE
open), `/api/explain`→400, `/readyz`→503.

---

## Wire format

The UI exchanges `job.Spec` as **JSON**; the store persists **YAML**. Node
positions are *not* persisted — on load the Builder auto-lays the DAG left-to-right
by topological order, keeping the saved spec clean.

`encoding/json` ignores yaml's `,inline`, so params are a nested object on the
wire (`{"type":"generator","params":{"rate":"1ms"}}`) and inline in the YAML file.

---

## Palette catalog

`job.Catalog()` is the **single source of truth** for the block palette and the
per-block param forms, served at `GET /api/palette`:

```go
type Param struct {
    Name     string
    Kind     string // string|int|number|duration|bool|enum|map
    Required bool
    Default  any
    Enum     []string
}
type BlockDef struct {
    Kind    string // source|operator|sink
    Type    string // generator|filter|tumbling|...
    Params  []Param
    Flusher bool
    Doc     string
}
type Palette struct{ Sources, Operators, Sinks []BlockDef }
```

Anti-drift: tests assert the catalog's type set equals the `buildSource`/
`buildOperator`/`buildSink` switches, and that each block's required+default
params produce a spec `job.Load` accepts.

---

## Endpoints

| Method + path | Purpose | Gate |
|---|---|---|
| `GET /api/palette` | block catalog for the builder | read |
| `GET /api/sample?stage=<label>` | recent records emitted by a stage (live data tail) | read |
| `GET /api/jobs` | list saved jobs (`JobInfo[]`) | read |
| `GET /api/jobs/{name}` | one spec as JSON | read |
| `POST /api/jobs` | save a spec (422 if invalid) | write |
| `DELETE /api/jobs/{name}` | delete a job | write |
| `POST /api/jobs/{name}/duplicate` | copy under `{newName}` | write |
| `POST /api/validate` | dry-run a spec → `{ok,error}` | write |
| `POST /api/run` | `{name}` → runner.Start (runs concurrently) | write |
| `POST /api/stop` | `{name}` → runner.Stop; empty body → StopAll | write |
| `GET /api/status` | `{running:[{name,started_at,uptime}], errors}` | read |
| `GET /api/graph?job=`,`/api/events?job=`,`/api/explain?job=`,`/api/sample?job=` | monitor a specific running job (idle-safe) | read |
| `GET /api/schemas`,`/api/dlq` | monitor (idle-safe) | read |
| `POST /api/ask` | `{subject,question,context}` → focused AI explanation of one element | read |
| `GET /healthz`,`/readyz` | probes (always open) | — |

**Multiple jobs run concurrently.** Each monitor endpoint takes `?job=<name>` to
select which running pipeline it reflects; the monitor view has a **job selector**
that drives the SSE stream's `?job`. With exactly one job running, `?job` may be
omitted (the runner picks the sole run).

Control-plane endpoints return 404 in monitor-only mode (no runner).

---

## Auth

`web.WithAuth(token)` (from `DRIFT_AUTH_TOKEN`):

- **unset → fail-open** — local/demo work with zero config.
- **set → fail-closed** — every request needs the token *except* `/healthz` and
  `/readyz` (probes stay open). Accept `Authorization: Bearer <token>` for `fetch`
  and `?token=<token>` for `EventSource` (browsers can't set SSE headers).
  Constant-time compare. The query-param-in-logs caveat is documented.

---

## Click-to-explain (contextual help)

Any block (palette, canvas node, inspector) and any monitor metric has a `?` that
opens a two-layer help popover (`static/help.js`):

1. **Instant** — a static description with no AI call: block/param docs from
   `job.Catalog()`, metric descriptions from a UI glossary. Works offline / with
   no API key.
2. **Ask AI** — a button that POSTs `{subject, question, context}` to `/api/ask`;
   the server calls `ai.Debugger.Ask` (Haiku, short answer). Context is the
   element's config (builder) or its live metric value (monitor), so answers are
   grounded in the user's actual pipeline. Requires `ANTHROPIC_API_KEY`.

## Hardening (Monitor)

- SSE reconnect with capped exponential backoff + jitter and a visible
  "reconnecting" banner (replaces the flat 3s retry).
- Loading skeletons until the first event; idle/empty state ("no pipeline running");
  error states on failed fetches.
- Responsive layout (grid → single column on narrow screens).
- Advanced-metrics drawer per stage: ProcessedTotal, ErrorTotal, p50/p99,
  QueueDepth, client-side throughput history, pipeline totals, uptime (from
  `/api/status`), plus **data inspection** (so you see records, not just numbers):
  - **Live record tail** — the last N actual payloads emitted by the stage, via a
    `pipeline.Tap` (bounded per-stage ring of output records) read at `/api/sample`.
  - **Aggregate sparkline** — the first numeric output field (e.g. a window's
    `count`/`sum`) plotted over the sampled records.
  - **Selectivity** — for a stage with one upstream, `kept X% (−N dropped)` from
    the two stages' `ProcessedTotal` (shown on the downstream stage = the edge's
    pass rate; surfaces filter/dedup selectivity).

---

## Required tests

| Test | Proves |
|---|---|
| `TestServer_Palette` | catalog served as JSON |
| `TestServer_JobsCRUD` | create/list/get/delete via HTTP |
| `TestServer_Validate` | good → ok; bad → error reported |
| `TestServer_RunStopStatus` | run → status running; stop by name → idle |
| `TestServer_MultipleJobsConcurrent` | two jobs run at once; stop one leaves the other |
| `TestServer_Sample` | `/api/sample?job=&stage=` returns the live record tail |
| `TestServer_Auth` | unset=open; set rejects missing, accepts bearer + `?token=` |
| `TestServer_EventsIdleEmitsState` | SSE emits idle state with no pipeline |
| `TestServer_MonitorFollowsCurrent` | graph reflects the runner's current pipeline |

(plus the existing `server_health_test.go`).

---

## See also

- [[Control Plane]] — runner + store this UI drives
- [[CLI & Jobs]] — `drift serve`
- [[AI Debugger]] — the explain endpoint
