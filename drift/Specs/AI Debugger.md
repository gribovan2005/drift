---
component: ai-debugger
status: stable
package: pkg/ai
file: pkg/ai/debugger.go
tested: true
---

# AI Debugger

On-demand pipeline diagnosis. Collects a metrics snapshot + graph, sends to Gemini, returns a structured plain-language explanation.

**This is a developer/ops tool — not in the data path.**

---

## Interface

```go
func New(apiKey, model string) *Debugger
// apiKey: reads GEMINI_API_KEY from env if empty
// model: defaults to "gemini-2.5-flash"

func (d *Debugger) Explain(ctx context.Context, snap metrics.MetricsSnapshot, graph []GraphNode) (string, error)
```

---

## MetricsSnapshot

```go
type MetricsSnapshot struct {
    CollectedAt time.Time
    Stages      []OperatorMetrics
}

type OperatorMetrics struct {
    Label          string
    QueueDepth     int64
    ProcessedTotal int64
    ErrorTotal     int64
    LatencyP50     time.Duration
    LatencyP99     time.Duration
    Throughput     float64       // records/sec
}
```

---

## Prompt structure

The prompt instructs Gemini to respond in exactly this format:

```
## Root Cause
One sentence. Name the stage and the metric.

## Findings
- [stage] observation with exact numbers

## Recommendations
1. Concrete action with stage name and why

## Config Snippet
Go code snippet if structural change needed (omit if not applicable)
```

**Why this format:** the Web UI renders each `## Section` as a distinct block. Structured output makes the response machine-parseable in future versions.

---

## API details

- Endpoint: `https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={apiKey}`
- Request: `{ contents: [{parts: [{text: prompt}]}], generationConfig: {maxOutputTokens: 1024} }`
- Response: `candidates[0].content.parts[0].text`

The `endpoint` field is overridable for tests (point at `httptest.Server`).

---

## Configuration

| Env var | Default | Description |
|---|---|---|
| `GEMINI_API_KEY` | required | Google AI API key |
| `DRIFT_AI_MODEL` | `gemini-2.5-flash` | Model to use |

Loaded automatically from `.env` file at startup via `internal/dotenv`.

---

## REST endpoint

`GET /api/explain` (served by `pkg/web`):
1. Calls `pipeline.Snapshot()` + `pipeline.Graph()`
2. Calls `debugger.Explain(ctx, snap, graph)`
3. Returns `text/plain` — the structured markdown response

---

## Required tests

| Test | What it proves |
|---|---|
| `TestDebugger_Explain_ReturnsAIText` | Parses Gemini response correctly |
| `TestDebugger_Explain_NoAPIKey` | Returns clear error when key missing |
| `TestDebugger_Explain_APIError` | Propagates HTTP error codes |
| `TestDebugger_Explain_ContextCancelled` | Respects context cancellation |

All tests use `httptest.Server` — no real API calls in CI.

---

## See also

- [[Overview#Key design decisions]]
- [[Architecture/Overview#Key design decisions]]
- [[Testing]]
