package ai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubServer mimics the Anthropic Messages API, returning a single text block.
func stubServer(t *testing.T, responseText string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "test-key", r.Header.Get("x-api-key"))

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":            "msg_test",
			"type":          "message",
			"role":          "assistant",
			"model":         "claude-opus-4-8",
			"stop_reason":   "end_turn",
			"stop_sequence": nil,
			"content": []map[string]any{
				{"type": "text", "text": responseText},
			},
			"usage": map[string]any{"input_tokens": 10, "output_tokens": 20},
		})
	}))
}

func sampleSnapshot() metrics.MetricsSnapshot {
	return metrics.MetricsSnapshot{
		CollectedAt: time.Now(),
		Stages: []metrics.OperatorMetrics{
			{
				Label:          "filter",
				ProcessedTotal: 1000,
				ErrorTotal:     0,
				Throughput:     5000,
				LatencyP50:     2 * time.Millisecond,
				LatencyP99:     8 * time.Millisecond,
			},
			{
				Label:          "map",
				QueueDepth:     200,
				ProcessedTotal: 800,
				ErrorTotal:     0,
				Throughput:     3500,
				LatencyP50:     5 * time.Millisecond,
				LatencyP99:     40 * time.Millisecond, // high p99 — potential bottleneck
			},
		},
	}
}

func TestDebugger_Ask_ReturnsAIText(t *testing.T) {
	want := "A tumbling window groups every N records and emits one aggregate."
	srv := stubServer(t, want)
	defer srv.Close()

	d := New("test-key", "")
	d.baseURL = srv.URL

	got, err := d.Ask(context.Background(), "operator: tumbling", "", "Configured params: {\"size\":50}")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestDebugger_Ask_NoAPIKey(t *testing.T) {
	d := &Debugger{model: defaultModel, baseURL: "http://unused"}
	_, err := d.Ask(context.Background(), "metric: p99", "", "")
	require.Error(t, err)
}

func TestDebugger_Explain_ReturnsAIText(t *testing.T) {
	want := "The map stage is the bottleneck — high p99 latency of 40ms."
	srv := stubServer(t, want)
	defer srv.Close()

	d := New("test-key", "")
	d.baseURL = srv.URL // point at mock

	graph := []GraphNode{
		{Label: "filter", Next: []string{"map"}},
		{Label: "map", Next: nil},
	}

	got, err := d.Explain(context.Background(), sampleSnapshot(), graph)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestDebugger_Explain_NoAPIKey(t *testing.T) {
	d := &Debugger{model: defaultModel, baseURL: "http://unused"}
	_, err := d.Explain(context.Background(), metrics.MetricsSnapshot{}, nil)
	assert.ErrorContains(t, err, "ANTHROPIC_API_KEY")
}

func TestDebugger_Explain_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	d := New("test-key", "")
	d.baseURL = srv.URL

	_, err := d.Explain(context.Background(), sampleSnapshot(), nil)
	assert.ErrorContains(t, err, "429")
}

func TestDebugger_Explain_ContextCancelled(t *testing.T) {
	// Server that blocks until the client disconnects.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	d := New("test-key", "")
	d.baseURL = srv.URL

	_, err := d.Explain(ctx, sampleSnapshot(), nil)
	assert.Error(t, err)
}
