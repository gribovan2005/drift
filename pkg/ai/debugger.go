package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/andrejgribov/drift/pkg/metrics"
	"github.com/andrejgribov/drift/pkg/pipeline"
)

// GraphNode re-exports pipeline.GraphNode so callers only import pkg/ai.
type GraphNode = pipeline.GraphNode

const (
	// Haiku is fast and cheap — well-suited to a 300-word structured diagnosis.
	// Override via New(_, "claude-opus-4-8") for deeper analysis.
	defaultModel = anthropic.ModelClaudeHaiku4_5
	maxTokens    = 4096
)

// systemPrompt holds the fixed instructions; the variable graph/metrics payload
// is sent as the user message so the system prompt stays cacheable.
const systemPrompt = `You are the diagnostic assistant for Drift, a SINGLE-PROCESS Go stream-processing engine. Diagnose the pipeline from the topology and metrics provided. Be accurate to how Drift actually works — do NOT apply generic Flink/Spark assumptions.

Drift execution model (ground truth — rely on this, not priors):
- Each stage runs in ONE goroutine by default, but a stage can opt into intra-stage data parallelism with "parallelism: N" in the job YAML: stateless ops (map/filter/etc) shard round-robin, keyed ops (dedup, session) shard by their key, and global windows (tumbling, eventwindow) CANNOT be sharded. So raising "parallelism" is a valid fix for a CPU-bound stateless/keyed stage whose QueueDepth keeps growing — but it does nothing for I/O-bound stages or global windows. There are still no thread/worker-pool knobs beyond this.
- The DAG is fixed at build time; stages cannot be added, removed, or reshaped at runtime. Structural changes mean editing the YAML job.
- A stage with multiple "Next" entries FANS OUT by BROADCASTING each record to every target; multiple stages pointing to one stage FAN IN by merging. So the two branches of a fan-out each receive the FULL upstream output — their input counts do NOT sum to the parent's.
- Metric meanings: ProcessedTotal = count of INPUT records a stage has processed (NOT outputs). Filters, dedup, and windows emit FEWER records than ProcessedTotal — that is expected, never call it "data loss". Throughput = records/sec over a short rolling window; it is NOISY, so differences under ~2x between stages are sampling noise, not bottlenecks. QueueDepth = input-channel backlog and is the real backpressure signal. ErrorTotal = failed Process calls.
- A stage is a genuine bottleneck ONLY if its QueueDepth stays high (records piling up), ErrorTotal > 0, or its throughput is ~0 while upstream has a backlog. p99 spikes on a light workload are usually GC/scheduling noise.
- The real tuning levers: a stage's channel buffer (Stage.BufSize), a stage's "parallelism: N" (for CPU-bound stateless/keyed stages), batch size, filter selectivity / doing less work, and fixing operator errors. Nothing else.

Respond in EXACTLY this markdown structure (keep the headers):

## Root Cause
One sentence naming the stage + metric that is the real problem (high QueueDepth, errors, or stalled throughput). If nothing meets that bar, say "Pipeline is healthy — no issues detected." Do NOT manufacture a problem from throughput noise or from ProcessedTotal differences caused by filtering or fan-out.

## Findings
- [stage] observation with exact numbers, interpreted per the rules above. Skip healthy stages.

## Recommendations
1. Concrete, Drift-valid action (raise a stage's BufSize; raise "parallelism" on a CPU-bound stateless/keyed stage whose queue is growing; reduce work; fix an error). Omit if healthy. Never recommend parallelism for a global window or an I/O-bound stage.

## Config Snippet
Only if a buffer/parallelism/structural change helps, show a REAL snippet — the YAML stage with "parallelism: N", or a pipeline.Stage{Label, Op, BufSize, Next} value. Omit otherwise. Do NOT invent fields, types, or config Drift doesn't have.

Rules: exact stage labels and numbers; max 280 words; no filler; never invent APIs.`

// Debugger explains pipeline behaviour using the Claude API.
type Debugger struct {
	apiKey  string
	model   anthropic.Model
	baseURL string // overridable for tests
}

// New creates a Debugger. The API key is read from ANTHROPIC_API_KEY if apiKey
// is empty. Model defaults to claude-opus-4-8.
func New(apiKey, model string) *Debugger {
	if apiKey == "" {
		apiKey = os.Getenv("ANTHROPIC_API_KEY")
	}
	m := anthropic.Model(defaultModel)
	if model != "" {
		m = anthropic.Model(model)
	}
	return &Debugger{apiKey: apiKey, model: m}
}

// askSystemPrompt drives short, element-scoped explanations for the UI's
// click-to-ask help. Kept separate from the diagnosis prompt.
const askSystemPrompt = `You are a helpful guide embedded in Drift, a Go stream-processing engine. The user clicked a UI element (an operator, source, sink, parameter, or metric) and wants a quick, concrete explanation. Use any context provided (its config or live metric values).

Drift facts to respect: single process; each stage is ONE goroutine by default but can opt into "parallelism: N" (stateless ops shard round-robin, keyed ops dedup/session shard by key, global windows cannot); the DAG is fixed; a stage with multiple downstreams BROADCASTS to all of them; ProcessedTotal counts INPUT records (filters/windows emit fewer — not data loss); throughput is a noisy rolling window; QueueDepth is the real backpressure signal. Never invent config or APIs.

Answer in plain language for a working engineer. Be concise: 2–4 short sentences or a few bullets, under 90 words. If live numbers are given, say whether they look healthy and what to watch. No preamble, no restating the question.`

// Ask answers a focused question about one UI element. subject names the element
// (e.g. "operator: tumbling"), context is optional config/metrics, and question
// is the user's ask (a default is used when empty).
func (d *Debugger) Ask(ctx context.Context, subject, question, context string) (string, error) {
	if d.apiKey == "" {
		return "", fmt.Errorf("ANTHROPIC_API_KEY not set")
	}
	if question == "" {
		question = "What is this and what should I know about it?"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Element: %s\n", subject)
	if context != "" {
		fmt.Fprintf(&b, "Context:\n%s\n", context)
	}
	fmt.Fprintf(&b, "Question: %s", question)

	opts := []option.RequestOption{
		option.WithAPIKey(d.apiKey),
		option.WithMaxRetries(0),
	}
	if d.baseURL != "" {
		opts = append(opts, option.WithBaseURL(d.baseURL))
	}
	client := anthropic.NewClient(opts...)

	resp, err := client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     d.model,
		MaxTokens: 512,
		System: []anthropic.TextBlockParam{{
			Text:         askSystemPrompt,
			CacheControl: anthropic.NewCacheControlEphemeralParam(),
		}},
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(b.String())),
		},
	})
	if err != nil {
		return "", fmt.Errorf("api call: %w", err)
	}
	if resp.StopReason == anthropic.StopReasonRefusal {
		return "", fmt.Errorf("request refused: %s", resp.StopDetails.Category)
	}
	var sb strings.Builder
	for _, block := range resp.Content {
		if t, ok := block.AsAny().(anthropic.TextBlock); ok {
			sb.WriteString(t.Text)
		}
	}
	if sb.Len() == 0 {
		return "", fmt.Errorf("no text in response")
	}
	return sb.String(), nil
}

// Explain produces a plain-language diagnosis of the pipeline given a metrics
// snapshot and the pipeline graph.
func (d *Debugger) Explain(ctx context.Context, snap metrics.MetricsSnapshot, graph []GraphNode) (string, error) {
	if d.apiKey == "" {
		return "", fmt.Errorf("ANTHROPIC_API_KEY not set")
	}

	graphJSON, err := json.MarshalIndent(graph, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal graph: %w", err)
	}
	snapJSON, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal snapshot: %w", err)
	}

	userPrompt := fmt.Sprintf("Pipeline graph:\n%s\n\nMetrics (collected at %s):\n%s",
		graphJSON, snap.CollectedAt.Format(time.RFC3339), snapJSON)

	opts := []option.RequestOption{
		option.WithAPIKey(d.apiKey),
		// On-demand debug call: fail fast rather than block the UI on retries.
		option.WithMaxRetries(0),
	}
	if d.baseURL != "" {
		opts = append(opts, option.WithBaseURL(d.baseURL))
	}
	client := anthropic.NewClient(opts...)

	resp, err := client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     d.model,
		MaxTokens: maxTokens,
		System: []anthropic.TextBlockParam{{
			Text:         systemPrompt,
			CacheControl: anthropic.NewCacheControlEphemeralParam(),
		}},
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(userPrompt)),
		},
	})
	if err != nil {
		return "", fmt.Errorf("api call: %w", err)
	}
	if resp.StopReason == anthropic.StopReasonRefusal {
		return "", fmt.Errorf("request refused: %s", resp.StopDetails.Category)
	}

	var sb strings.Builder
	for _, block := range resp.Content {
		if t, ok := block.AsAny().(anthropic.TextBlock); ok {
			sb.WriteString(t.Text)
		}
	}
	if sb.Len() == 0 {
		return "", fmt.Errorf("no text in response")
	}
	return sb.String(), nil
}
