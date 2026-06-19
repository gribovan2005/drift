package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/andrejgribov/drift/pkg/metrics"
	"github.com/andrejgribov/drift/pkg/pipeline"
)

// GraphNode re-exports pipeline.GraphNode so callers only import pkg/ai.
type GraphNode = pipeline.GraphNode

const (
	defaultModel   = "gemini-2.5-flash"
	geminiEndpoint = "https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent?key=%s"
	maxTokens      = 1024
)

// Debugger explains pipeline behaviour using Gemini.
type Debugger struct {
	apiKey   string
	model    string
	endpoint string     // overridable for tests
	client   *http.Client
}

// New creates a Debugger. API key is read from GEMINI_API_KEY if apiKey is
// empty. Model defaults to gemini-2.0-flash.
func New(apiKey, model string) *Debugger {
	if apiKey == "" {
		apiKey = os.Getenv("GEMINI_API_KEY")
	}
	if model == "" {
		model = defaultModel
	}
	return &Debugger{
		apiKey:   apiKey,
		model:    model,
		endpoint: geminiEndpoint,
		client:   &http.Client{Timeout: 30 * time.Second},
	}
}

// Explain produces a plain-language diagnosis of the pipeline given a metrics
// snapshot and the pipeline graph.
func (d *Debugger) Explain(ctx context.Context, snap metrics.MetricsSnapshot, graph []GraphNode) (string, error) {
	if d.apiKey == "" {
		return "", fmt.Errorf("GEMINI_API_KEY not set")
	}

	graphJSON, err := json.MarshalIndent(graph, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal graph: %w", err)
	}
	snapJSON, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal snapshot: %w", err)
	}

	prompt := fmt.Sprintf(`You are a Drift streaming pipeline debugger. Analyse the topology and metrics below, then respond in EXACTLY this structure (keep the markdown headers):

## Root Cause
One sentence. Name the specific stage and metric that is the primary problem. If everything is healthy, say "Pipeline is healthy — no issues detected."

## Findings
- [stage-name] Specific observation with exact numbers (queue depth, latency, throughput, error count).
- Repeat for each noteworthy stage. Skip healthy stages unless context is needed.

## Recommendations
1. Concrete action targeting the root cause — name the stage, what to change, why.
2. Follow-up action if applicable.
(Omit if pipeline is healthy.)

## Config Snippet
If you recommend a structural change (e.g. increase buffer, split stage, add parallelism), show a short Go code snippet illustrating the change. Omit if not applicable.

Rules: use exact stage labels and numbers from the metrics. Max 300 words. No filler text.

Pipeline graph:
%s

Metrics (collected at %s):
%s`, graphJSON, snap.CollectedAt.Format(time.RFC3339), snapJSON)

	body, err := json.Marshal(map[string]any{
		"contents": []map[string]any{
			{"parts": []map[string]string{{"text": prompt}}},
		},
		"generationConfig": map[string]any{
			"maxOutputTokens": maxTokens,
		},
	})
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}

	url := d.endpoint
	if d.endpoint == geminiEndpoint {
		url = fmt.Sprintf(d.endpoint, d.model, d.apiKey)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("api call: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("api error %d: %s", resp.StatusCode, respBytes)
	}

	var apiResp struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(respBytes, &apiResp); err != nil {
		return "", fmt.Errorf("parse response: %w", err)
	}
	if len(apiResp.Candidates) > 0 && len(apiResp.Candidates[0].Content.Parts) > 0 {
		return apiResp.Candidates[0].Content.Parts[0].Text, nil
	}
	return "", fmt.Errorf("no text in response")
}
