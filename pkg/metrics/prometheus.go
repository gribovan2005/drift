package metrics

import (
	"bufio"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// PrometheusContentType is the exposition-format content type served by Handler.
const PrometheusContentType = "text/plain; version=0.0.4; charset=utf-8"

// Snapshotter yields a MetricsSnapshot. *pipeline.Pipeline satisfies it
// structurally, so the exporter needs no import of pkg/pipeline (no cycle).
type Snapshotter interface {
	Snapshot() MetricsSnapshot
}

// promMetric describes one exported series family.
type promMetric struct {
	name  string
	help  string
	typ   string // "counter" | "gauge"
	value func(OperatorMetrics) string
}

// promFamilies is the public metric contract. Order is stable.
var promFamilies = []promMetric{
	{"drift_stage_processed_total", "Total records processed by the stage.", "counter",
		func(m OperatorMetrics) string { return strconv.FormatInt(m.ProcessedTotal, 10) }},
	{"drift_stage_errors_total", "Total processing errors in the stage.", "counter",
		func(m OperatorMetrics) string { return strconv.FormatInt(m.ErrorTotal, 10) }},
	{"drift_stage_queue_depth", "Current input queue depth of the stage.", "gauge",
		func(m OperatorMetrics) string { return strconv.FormatInt(m.QueueDepth, 10) }},
	{"drift_stage_throughput_records_per_second", "Recent throughput of the stage in records/sec.", "gauge",
		func(m OperatorMetrics) string { return strconv.FormatFloat(m.Throughput, 'g', -1, 64) }},
	{"drift_stage_latency_p50_seconds", "Median per-batch processing latency, seconds.", "gauge",
		func(m OperatorMetrics) string { return strconv.FormatFloat(m.LatencyP50.Seconds(), 'g', -1, 64) }},
	{"drift_stage_latency_p99_seconds", "p99 per-batch processing latency, seconds.", "gauge",
		func(m OperatorMetrics) string { return strconv.FormatFloat(m.LatencyP99.Seconds(), 'g', -1, 64) }},
}

// WritePrometheus writes snap in the Prometheus text exposition format. Each
// metric family is emitted with its HELP/TYPE header followed by one line per
// stage, labelled stage="<label>".
func WritePrometheus(w io.Writer, snap MetricsSnapshot) error {
	bw := bufio.NewWriter(w)
	for _, fam := range promFamilies {
		if _, err := io.WriteString(bw, "# HELP "+fam.name+" "+fam.help+"\n"); err != nil {
			return err
		}
		if _, err := io.WriteString(bw, "# TYPE "+fam.name+" "+fam.typ+"\n"); err != nil {
			return err
		}
		for _, s := range snap.Stages {
			line := fam.name + `{stage="` + escapeLabel(s.Label) + `"} ` + fam.value(s) + "\n"
			if _, err := io.WriteString(bw, line); err != nil {
				return err
			}
		}
	}
	return bw.Flush()
}

// Handler returns an http.Handler that scrapes src on each request and serves the
// Prometheus text exposition format. Mount it at /metrics.
func Handler(src Snapshotter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", PrometheusContentType)
		_ = WritePrometheus(w, src.Snapshot())
	})
}

// escapeLabel escapes a Prometheus label value: backslash, double-quote, newline.
func escapeLabel(s string) string {
	if !strings.ContainsAny(s, "\\\"\n") {
		return s
	}
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(s)
}
