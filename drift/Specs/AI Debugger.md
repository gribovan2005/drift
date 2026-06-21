---
component: ai-debugger
status: stable
package: pkg/ai
file: pkg/ai/debugger.go
tested: true
---

# AI Debugger

On-demand pipeline diagnosis. Collects a metrics snapshot + graph, sends to Claude, returns a structured plain-language explanation.

**This is a developer/ops tool — not in the data path.**

---

## Interface

```go
func New(apiKey, model string) *Debugger
// apiKey: reads ANTHROPIC_API_KEY from env if empty
// model: defaults to "claude-haiku-4-5" (fast + cheap; pass "claude-opus-4-8" for deeper analysis)

func (d *Debugger) Explain(ctx context.Context, snap metrics.MetricsSnapshot, graph []GraphNode) (string, error)

// Ask answers a focused question about one UI element (operator/source/sink/
// param/metric). subject names it, context is optional config or live metric
// values, question defaults when empty. Powers the UI's click-to-explain help
// (POST /api/ask). Short answer (<90 words), own concise system prompt.
func (d *Debugger) Ask(ctx context.Context, subject, question, context string) (string, error)
```

Uses the official Anthropic Go SDK (`github.com/anthropics/anthropic-sdk-go`).

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

Fixed instructions live in the **system prompt** (cached via `CacheControl`); the
variable graph + metrics payload is sent as the **user message**. Claude responds
in exactly this format:

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

- SDK: `client.Messages.New(ctx, MessageNewParams{...})` via `github.com/anthropics/anthropic-sdk-go`
- Model: `anthropic.ModelClaudeHaiku4_5` (default, cheap/fast), `MaxTokens: 4096`
- No extended thinking — Haiku 4.5 is pre-4.6 (adaptive thinking is 4.6+ only), and a 300-word structured diagnosis doesn't need it. Pass an Opus model via `New` if deeper reasoning is wanted.
- System prompt cached with `CacheControl: NewCacheControlEphemeralParam()`
- `MaxRetries: 0` — on-demand debug call fails fast rather than blocking the UI
- Refusal handling: `StopReasonRefusal` → returns an error with the refusal category
- Response: concatenates all `TextBlock` content (thinking blocks skipped)

The `baseURL` field is overridable for tests (point at `httptest.Server` via `option.WithBaseURL`).

---

## Configuration

| Env var | Default | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | required | Anthropic API key |
| `DRIFT_AI_MODEL` | `claude-haiku-4-5` | Model to use |

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
| `TestDebugger_Explain_ReturnsAIText` | Parses Claude response correctly |
| `TestDebugger_Explain_NoAPIKey` | Returns clear error when key missing |
| `TestDebugger_Explain_APIError` | Propagates HTTP error codes |
| `TestDebugger_Explain_ContextCancelled` | Respects context cancellation |
| `TestDebugger_Ask_ReturnsAIText` | Element-scoped Ask parses the response |
| `TestDebugger_Ask_NoAPIKey` | Ask returns a clear error when key missing |

All tests use `httptest.Server` (via `option.WithBaseURL`) — no real API calls in CI.

---

## See also

- [[Overview#Key design decisions]]
- [[Architecture/Overview#Key design decisions]]
- [[Testing]]
