---
component: metrics-export
status: stable
package: pkg/metrics, sdk
tested: true
---

# Metrics Export (Prometheus)

Drift exposes per-stage runtime metrics in the **Prometheus text exposition
format** so an embedded SDK pipeline can be scraped by the host service's existing
observability stack — no cluster, no separate agent. Built **dependency-free**
(stdlib only): the exporter formats a `metrics.MetricsSnapshot` by hand, so SDK
users are not forced to pull in `client_golang`.

Source of truth is `pipeline.Pipeline.Snapshot() metrics.MetricsSnapshot` — the
same snapshot the web dashboard reads. The exporter is a second consumer of it.

---

## API (`pkg/metrics`)

```go
// Snapshotter is anything that yields a snapshot — *pipeline.Pipeline satisfies
// it structurally (no import of pkg/pipeline → no cycle).
type Snapshotter interface{ Snapshot() MetricsSnapshot }

func WritePrometheus(w io.Writer, snap MetricsSnapshot) error  // format one snapshot
func Handler(src Snapshotter) http.Handler                      // scrape src per request
```

## API (`sdk` facade)

```go
func PrometheusHandler(p *pipeline.Pipeline) http.Handler  // = metrics.Handler(p)
```

Usage from an embedding service:

```go
p, _ := sdk.New().From(src).Map(fn).To(sink).Build()
http.Handle("/metrics", sdk.PrometheusHandler(p))
go p.Run(ctx)
```

## Standalone

`pkg/web` mounts `GET /metrics` (always, auth-exempt like the health probes),
serving `provider.Current("").Snapshot()`. When idle (no pipeline) it returns an
empty body with HTTP 200 — a valid empty scrape, not an error.

---

## Exposed series

All gauges/counters are labelled `stage="<label>"` (the pipeline stage label).
One metric family per line group, each preceded by `# HELP` / `# TYPE`.

| Metric | Type | Source field |
|---|---|---|
| `drift_stage_processed_total` | counter | `ProcessedTotal` |
| `drift_stage_errors_total` | counter | `ErrorTotal` |
| `drift_stage_queue_depth` | gauge | `QueueDepth` |
| `drift_stage_throughput_records_per_second` | gauge | `Throughput` |
| `drift_stage_latency_p50_seconds` | gauge | `LatencyP50` (→ seconds) |
| `drift_stage_latency_p99_seconds` | gauge | `LatencyP99` (→ seconds) |

- Durations are emitted in **seconds** (Prometheus convention), float.
- Latency p50/p99 are emitted as two explicit gauges (not a faked summary) — we
  only have the quantiles, not sum/count.
- Counter names end in `_total`.
- Label values are escaped (`\`, `"`, `\n`) so arbitrary `ApplyLabeled` labels are
  safe.
- Content-Type: `text/plain; version=0.0.4; charset=utf-8`.

---

## Invariants

1. **Dependency-free** — `pkg/metrics` gains no third-party imports; format is
   hand-written stdlib. `pkg/core`'s no-import rule is unaffected.
2. **Snapshot is the single source** — the exporter never reads stage state
   directly; it consumes `MetricsSnapshot`, identical to the dashboard.
3. **Idle-safe** — a nil/empty snapshot yields a valid (possibly empty) scrape, HTTP 200.
4. **Stable names** — metric names/labels are part of the public contract; renames
   are breaking.

---

## Required tests (`pkg/metrics`, `sdk`, `pkg/web`)

- `WritePrometheus_Format` — golden-ish check: HELP/TYPE present, one line per
  stage per family, counter names end `_total`, durations in seconds.
- `WritePrometheus_EscapesLabels` — a label with `"`/`\` is escaped.
- `WritePrometheus_Empty` — empty snapshot → no series, no panic.
- `Handler_ServesSnapshot` — httptest GET returns 200, correct Content-Type, body
  contains expected metric names (real in-process pipeline, no mocks).
- `sdk.PrometheusHandler` — end-to-end: build a pipeline, scrape, see stage metrics.
- `pkg/web`: `GET /metrics` returns 200 with metrics; idle returns 200 empty.

---

## See also

- [[AI Debugger]] — the other consumer of `MetricsSnapshot`
- [[SDK]] — `sdk.PrometheusHandler`
- [[Web UI & Builder]] — dashboard, the third consumer
