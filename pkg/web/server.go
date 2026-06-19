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

	"github.com/andrejgribov/drift/pkg/ai"
	"github.com/andrejgribov/drift/pkg/dlq"
	"github.com/andrejgribov/drift/pkg/pipeline"
	"github.com/andrejgribov/drift/pkg/schema"
)

//go:embed static
var staticFiles embed.FS

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

// Server exposes the Drift Web UI and API over HTTP.
type Server struct {
	addr     string
	pipe     *pipeline.Pipeline
	registry *schema.Registry
	debugger *ai.Debugger
	dlq      *dlq.Queue   // optional
	logger   *slog.Logger
}

// New creates a Server.
func New(addr string, p *pipeline.Pipeline, reg *schema.Registry, dbg *ai.Debugger, opts ...ServerOption) *Server {
	s := &Server{
		addr:     addr,
		pipe:     p,
		registry: reg,
		debugger: dbg,
		logger:   slog.Default(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// ListenAndServe starts the HTTP server and blocks until ctx is cancelled.
func (s *Server) ListenAndServe(ctx context.Context) error {
	mux := http.NewServeMux()

	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		return fmt.Errorf("embed sub: %w", err)
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))

	// Health
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)

	// API
	mux.HandleFunc("/api/graph", s.handleGraph)
	mux.HandleFunc("/api/schemas", s.handleSchemas)
	mux.HandleFunc("/api/events", s.handleEvents)
	mux.HandleFunc("/api/explain", s.handleExplain)
	mux.HandleFunc("/api/dlq", s.handleDLQ)

	srv := &http.Server{Addr: s.addr, Handler: mux}
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

// handleReadyz returns 200 once the pipeline has processed at least one
// record. Returns 503 before that. Used for readiness probes.
func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if s.pipe.IsReady() {
		writeJSON(w, map[string]string{"status": "ok"})
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	writeJSON(w, map[string]string{"status": "not ready"})
}

func (s *Server) handleGraph(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.pipe.Graph())
}

func (s *Server) handleSchemas(w http.ResponseWriter, _ *http.Request) {
	result := map[string]any{}
	for _, id := range s.registry.SchemaIDs() {
		result[id] = s.registry.AllVersions(id)
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

// handleEvents streams MetricsSnapshot as Server-Sent Events, once per second.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			snap := s.pipe.Snapshot()
			payload := map[string]any{
				"snapshot": snap,
				"graph":    s.pipe.Graph(),
			}
			if s.dlq != nil {
				_, total := s.dlq.Snapshot()
				payload["dlq_total"] = total
			}
			data, err := json.Marshal(payload)
			if err != nil {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

// handleExplain calls the AI debugger and returns the explanation.
func (s *Server) handleExplain(w http.ResponseWriter, r *http.Request) {
	snap := s.pipe.Snapshot()
	graph := s.pipe.Graph()
	explanation, err := s.debugger.Explain(r.Context(), snap, graph)
	if err != nil {
		s.logger.Error("ai explain failed", "err", err)
		http.Error(w, fmt.Sprintf("AI error: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprint(w, explanation)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}
