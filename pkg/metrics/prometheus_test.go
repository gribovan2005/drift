package metrics

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func sampleSnapshot() MetricsSnapshot {
	return MetricsSnapshot{
		CollectedAt: time.Now(),
		Stages: []OperatorMetrics{
			{
				Label:          "map-1",
				QueueDepth:     3,
				ProcessedTotal: 100,
				ErrorTotal:     2,
				LatencyP50:     500 * time.Microsecond,
				LatencyP99:     2 * time.Millisecond,
				Throughput:     1234.5,
			},
			{
				Label:          "filter-2",
				QueueDepth:     0,
				ProcessedTotal: 50,
				ErrorTotal:     0,
				Throughput:     0,
			},
		},
	}
}

func TestWritePrometheus_Format(t *testing.T) {
	var buf bytes.Buffer
	if err := WritePrometheus(&buf, sampleSnapshot()); err != nil {
		t.Fatalf("write: %v", err)
	}
	out := buf.String()

	mustContain := []string{
		"# HELP drift_stage_processed_total",
		"# TYPE drift_stage_processed_total counter",
		`drift_stage_processed_total{stage="map-1"} 100`,
		`drift_stage_processed_total{stage="filter-2"} 50`,
		"# TYPE drift_stage_errors_total counter",
		`drift_stage_errors_total{stage="map-1"} 2`,
		"# TYPE drift_stage_queue_depth gauge",
		`drift_stage_queue_depth{stage="map-1"} 3`,
		"# TYPE drift_stage_throughput_records_per_second gauge",
		`drift_stage_throughput_records_per_second{stage="map-1"} 1234.5`,
		// 500µs = 0.0005s
		`drift_stage_latency_p50_seconds{stage="map-1"} 0.0005`,
		// 2ms = 0.002s
		`drift_stage_latency_p99_seconds{stage="map-1"} 0.002`,
	}
	for _, want := range mustContain {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n--- full output ---\n%s", want, out)
		}
	}

	// Counter names must end with _total.
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "# TYPE") && strings.HasSuffix(line, "counter") {
			name := strings.Fields(line)[2]
			if !strings.HasSuffix(name, "_total") {
				t.Errorf("counter %q does not end in _total", name)
			}
		}
	}
}

func TestWritePrometheus_EscapesLabels(t *testing.T) {
	snap := MetricsSnapshot{Stages: []OperatorMetrics{
		{Label: `weird"\stage`, ProcessedTotal: 1},
	}}
	var buf bytes.Buffer
	if err := WritePrometheus(&buf, snap); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !strings.Contains(buf.String(), `stage="weird\"\\stage"`) {
		t.Fatalf("label not escaped:\n%s", buf.String())
	}
}

func TestWritePrometheus_Empty(t *testing.T) {
	var buf bytes.Buffer
	if err := WritePrometheus(&buf, MetricsSnapshot{}); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Headers present, but no series lines.
	out := buf.String()
	if !strings.Contains(out, "# TYPE drift_stage_processed_total counter") {
		t.Fatal("expected family headers even when empty")
	}
	if strings.Contains(out, "{stage=") {
		t.Fatalf("expected no series for empty snapshot:\n%s", out)
	}
}

// fakeSource lets us test Handler without importing pkg/pipeline.
type fakeSource struct{ snap MetricsSnapshot }

func (f fakeSource) Snapshot() MetricsSnapshot { return f.snap }

func TestHandler_ServesSnapshot(t *testing.T) {
	h := Handler(fakeSource{sampleSnapshot()})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != PrometheusContentType {
		t.Fatalf("content-type %q, want %q", ct, PrometheusContentType)
	}
	if !strings.Contains(rec.Body.String(), `drift_stage_processed_total{stage="map-1"} 100`) {
		t.Fatalf("body missing expected metric:\n%s", rec.Body.String())
	}
}
