package web

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"github.com/gribovan2005/drift/pkg/ai"
	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/dlq"
	"github.com/gribovan2005/drift/pkg/job"
	"github.com/gribovan2005/drift/pkg/metrics"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/runner"
	"github.com/gribovan2005/drift/pkg/schema"
)

//go:embed static
var staticFiles embed.FS

// PipelineProvider yields the pipeline the monitor endpoints should reflect for
// a given job name. It lets the server monitor either a fixed pipeline (legacy,
// ignores the name) or one of the runner's concurrently running pipelines.
// Current returns nil when that job isn't running.
type PipelineProvider interface {
	Current(job string) *pipeline.Pipeline
}

// staticProvider always returns the same pipeline (legacy monitor-only mode).
type staticProvider struct{ p *pipeline.Pipeline }

func (s staticProvider) Current(string) *pipeline.Pipeline { return s.p }

// ServerOption configures a Server.
type ServerOption func(*Server)

// WithDLQ wires a dead-letter queue into the server's /api/dlq endpoint
// and includes the DLQ count in SSE events.
func WithDLQ(q *dlq.Queue) ServerOption {
	return func(s *Server) { s.dlq = q }
}

// WithServerLogger sets the logger for HTTP request errors.
func WithServerLogger(l *slog.Logger) ServerOption {
	return func(s *Server) { s.logger = l }
}

// WithRunner enables the control plane: the server monitors the runner's current
// pipeline and exposes the builder/job-management API. Without it the server is
// monitor-only over the fixed pipeline passed to New.
func WithRunner(r *runner.Runner) ServerOption {
	return func(s *Server) {
		s.rnr = r
		s.provider = r
	}
}

// WithAuth requires a bearer token (or ?token= for SSE) on every request except
// the health probes. An empty token disables auth (fail-open for local/demo).
func WithAuth(token string) ServerOption {
	return func(s *Server) { s.token = token }
}

// Server exposes the Drift Web UI and API over HTTP.
type Server struct {
	addr     string
	provider PipelineProvider
	rnr      *runner.Runner // optional; enables control-plane endpoints
	registry *schema.Registry
	debugger *ai.Debugger
	dlq      *dlq.Queue // optional
	token    string     // optional; "" disables auth
	logger   *slog.Logger
}

// New creates a Server monitoring a fixed pipeline. Use WithRunner to manage
// pipelines at runtime instead.
func New(addr string, p *pipeline.Pipeline, reg *schema.Registry, dbg *ai.Debugger, opts ...ServerOption) *Server {
	s := &Server{
		addr:     addr,
		provider: staticProvider{p},
		registry: reg,
		debugger: dbg,
		logger:   slog.Default(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// routes builds the HTTP handler (mux + auth middleware). Split out so tests can
// exercise the full request path including auth.
func (s *Server) routes() (http.Handler, error) {
	mux := http.NewServeMux()

	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		return nil, fmt.Errorf("embed sub: %w", err)
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))

	// Health + metrics (auth-exempt, for orchestrators / Prometheus)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("GET /metrics", s.handleMetrics)

	// Monitor API
	mux.HandleFunc("/api/graph", s.handleGraph)
	mux.HandleFunc("/api/schemas", s.handleSchemas)
	mux.HandleFunc("/api/events", s.handleEvents)
	mux.HandleFunc("/api/explain", s.handleExplain)
	mux.HandleFunc("POST /api/ask", s.handleAsk)
	mux.HandleFunc("/api/dlq", s.handleDLQ)

	// Control-plane API (only when a runner is wired)
	if s.rnr != nil {
		mux.HandleFunc("GET /api/palette", s.handlePalette)
		mux.HandleFunc("GET /api/status", s.handleStatus)
		mux.HandleFunc("GET /api/sample", s.handleSample)
		mux.HandleFunc("GET /api/jobs", s.handleJobsList)
		mux.HandleFunc("POST /api/jobs", s.handleJobSave)
		mux.HandleFunc("GET /api/jobs/{name}", s.handleJobGet)
		mux.HandleFunc("DELETE /api/jobs/{name}", s.handleJobDelete)
		mux.HandleFunc("POST /api/jobs/{name}/duplicate", s.handleJobDuplicate)
		mux.HandleFunc("POST /api/validate", s.handleValidate)
		mux.HandleFunc("POST /api/run", s.handleRun)
		mux.HandleFunc("POST /api/stop", s.handleStop)
	}

	return s.withAuth(mux), nil
}

// ListenAndServe starts the HTTP server and blocks until ctx is cancelled.
func (s *Server) ListenAndServe(ctx context.Context) error {
	handler, err := s.routes()
	if err != nil {
		return err
	}

	srv := &http.Server{Addr: s.addr, Handler: handler}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutCtx) //nolint:errcheck
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// handleHealthz always returns 200. Used for liveness probes.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"})
}

// handleMetrics serves the current pipeline's metrics in the Prometheus text
// exposition format. When idle (no pipeline) it returns an empty 200 — a valid
// empty scrape. Auth-exempt so a scraper can reach it like the health probes.
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", metrics.PrometheusContentType)
	if p := s.provider.Current(""); p != nil {
		_ = metrics.WritePrometheus(w, p.Snapshot())
	}
}

// handleReadyz returns 200 once a pipeline is running and has processed at least
// one record. Returns 503 otherwise. Used for readiness probes.
func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if p := s.provider.Current(""); p != nil && p.IsReady() {
		writeJSON(w, map[string]string{"status": "ok"})
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	writeJSON(w, map[string]string{"status": "not ready"})
}

func (s *Server) handleGraph(w http.ResponseWriter, r *http.Request) {
	p := s.provider.Current(r.URL.Query().Get("job"))
	if p == nil {
		writeJSON(w, []pipeline.GraphNode{})
		return
	}
	writeJSON(w, p.Graph())
}

func (s *Server) handleSchemas(w http.ResponseWriter, _ *http.Request) {
	result := map[string]any{}
	if s.registry != nil {
		for _, id := range s.registry.SchemaIDs() {
			result[id] = s.registry.AllVersions(id)
		}
	}
	writeJSON(w, result)
}

// handleDLQ returns recent failed records. Returns 404 if no DLQ is wired.
func (s *Server) handleDLQ(w http.ResponseWriter, _ *http.Request) {
	if s.dlq == nil {
		http.Error(w, "DLQ not configured", http.StatusNotFound)
		return
	}
	records, total := s.dlq.Snapshot()
	writeJSON(w, map[string]any{
		"total":   total,
		"records": records,
	})
}

// handleEvents streams pipeline state as Server-Sent Events, once per second.
// When no pipeline is running it emits {"state":"idle"} to keep the stream alive.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("X-Accel-Buffering", "no") // disable proxy buffering of SSE

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	job := r.URL.Query().Get("job")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	send := func() bool {
		payload := map[string]any{}
		if p := s.provider.Current(job); p != nil {
			payload["state"] = "running"
			payload["snapshot"] = p.Snapshot()
			payload["graph"] = p.Graph()
		} else {
			payload["state"] = "idle"
		}
		if s.dlq != nil {
			_, total := s.dlq.Snapshot()
			payload["dlq_total"] = total
		}
		data, err := json.Marshal(payload)
		if err != nil {
			return false
		}
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
		return true
	}

	send() // immediate first frame so the UI doesn't wait a second
	for {
		select {
		case <-ticker.C:
			if !send() {
				return
			}
		case <-r.Context().Done():
			return
		}
	}
}

// handleExplain calls the AI debugger and returns the explanation.
func (s *Server) handleExplain(w http.ResponseWriter, r *http.Request) {
	p := s.provider.Current(r.URL.Query().Get("job"))
	if p == nil {
		http.Error(w, "no pipeline running", http.StatusBadRequest)
		return
	}
	if s.debugger == nil {
		http.Error(w, "AI debugger not configured", http.StatusBadRequest)
		return
	}
	explanation, err := s.debugger.Explain(r.Context(), p.Snapshot(), p.Graph())
	if err != nil {
		s.logger.Error("ai explain failed", "err", err)
		http.Error(w, fmt.Sprintf("AI error: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprint(w, explanation)
}

// handleAsk answers a focused question about one UI element via the AI debugger.
func (s *Server) handleAsk(w http.ResponseWriter, r *http.Request) {
	if s.debugger == nil {
		http.Error(w, "AI debugger not configured", http.StatusBadRequest)
		return
	}
	var body struct {
		Subject  string `json:"subject"`
		Question string `json:"question"`
		Context  string `json:"context"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	answer, err := s.debugger.Ask(r.Context(), body.Subject, body.Question, body.Context)
	if err != nil {
		s.logger.Error("ai ask failed", "err", err)
		http.Error(w, fmt.Sprintf("AI error: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprint(w, answer)
}

// ── control-plane handlers ──────────────────────────────────────────────────

func (s *Server) handlePalette(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, job.Catalog())
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.rnr.Status())
}

// handleSample returns recent records emitted by a stage (the live data tail).
func (s *Server) handleSample(w http.ResponseWriter, r *http.Request) {
	job := r.URL.Query().Get("job")
	stage := r.URL.Query().Get("stage")
	records := s.rnr.Sample(job, stage)
	if records == nil {
		records = []core.Record{}
	}
	writeJSON(w, map[string]any{"job": job, "stage": stage, "records": records})
}

func (s *Server) handleJobsList(w http.ResponseWriter, _ *http.Request) {
	list, err := s.rnr.Store().List()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, list)
}

func (s *Server) handleJobGet(w http.ResponseWriter, r *http.Request) {
	spec, err := s.rnr.Store().Load(r.PathValue("name"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, spec)
}

func (s *Server) handleJobSave(w http.ResponseWriter, r *http.Request) {
	var spec job.Spec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.rnr.Store().Save(spec); err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	writeJSON(w, map[string]any{"ok": true, "name": spec.Name})
}

func (s *Server) handleJobDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.rnr.Store().Delete(r.PathValue("name")); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func (s *Server) handleJobDuplicate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		NewName string `json:"new_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.rnr.Store().Duplicate(r.PathValue("name"), body.NewName); err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	writeJSON(w, map[string]any{"ok": true, "name": body.NewName})
}

func (s *Server) handleValidate(w http.ResponseWriter, r *http.Request) {
	var spec job.Spec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	data, err := job.Marshal(spec)
	if err == nil {
		_, err = job.Load(data)
	}
	if err != nil {
		writeJSON(w, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.rnr.Start(body.Name); err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, s.rnr.Status())
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	// Body is optional; an empty name stops all running jobs.
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.Name == "" {
		s.rnr.StopAll()
	} else if err := s.rnr.Stop(body.Name); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, s.rnr.Status())
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func writeErr(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()}) //nolint:errcheck
}
