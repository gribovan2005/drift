package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/andrejgribov/drift/internal/dotenv"
	"github.com/andrejgribov/drift/pkg/ai"
	"github.com/andrejgribov/drift/pkg/core"
	"github.com/andrejgribov/drift/pkg/operator"
	"github.com/andrejgribov/drift/pkg/pipeline"
	"github.com/andrejgribov/drift/pkg/schema"
	"github.com/andrejgribov/drift/pkg/sink"
	"github.com/andrejgribov/drift/pkg/source"
)

func main() {
	dotenv.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := schema.NewRegistry()

	v1 := core.Schema{
		ID:      "events",
		Version: 1,
		Fields: []core.Field{
			{Name: "id", Type: core.FieldTypeString},
			{Name: "amount", Type: core.FieldTypeFloat},
			{Name: "region", Type: core.FieldTypeString},
		},
	}
	if err := reg.Register(v1); err != nil {
		log.Fatal(err)
	}

	adapter := operator.NewSchemaAdapter(v1, operator.AliasMap{"amount": "value"})
	reg.Subscribe("events", adapter)

	stages := []pipeline.Stage{
		{
			Label: "filter-small",
			Op: operator.NewFilter(func(r core.Record) bool {
				amt, _ := r.Payload["amount"].(float64)
				return amt >= 10.0
			}),
		},
		{Label: "schema-adapt", Op: adapter},
		{
			Label: "enrich",
			Op: operator.NewMap(func(r core.Record) (core.Record, error) {
				r.Payload["processed_at"] = time.Now().Unix()
				return r, nil
			}),
		},
	}

	regions := []string{"eu-west", "us-east", "ap-south"}
	src := source.NewGenerator(func(seq int) core.Record {
		return core.Record{
			SchemaID:      "events",
			SchemaVersion: 1,
			Payload: map[string]any{
				"id":     fmt.Sprintf("tx-%d", seq),
				"amount": float64(seq%100) + 0.5,
				"region": regions[seq%len(regions)],
			},
		}
	}, time.Millisecond) // 1000 records/sec

	snk := sink.NewMemory()
	p := pipeline.New(src, stages, snk)

	debugger := ai.New("", "") // reads GEMINI_API_KEY from env

	// Start pipeline in background.
	pipelineErr := make(chan error, 1)
	go func() { pipelineErr <- p.Run(ctx) }()

	// HTTP server for metrics and AI debug endpoint.
	mux := http.NewServeMux()

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		snap := p.Snapshot()
		w.Header().Set("content-type", "application/json")
		json.NewEncoder(w).Encode(snap)
	})

	mux.HandleFunc("/debug/explain", func(w http.ResponseWriter, r *http.Request) {
		snap := p.Snapshot()
		graph := p.Graph()
		explanation, err := debugger.Explain(r.Context(), snap, graph)
		if err != nil {
			http.Error(w, fmt.Sprintf("AI error: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("content-type", "text/plain; charset=utf-8")
		fmt.Fprint(w, explanation)
	})

	mux.HandleFunc("/schema/evolve", func(w http.ResponseWriter, r *http.Request) {
		// Demo: publish schema v2 to trigger live evolution.
		v2 := core.Schema{
			ID:      "events",
			Version: 2,
			Fields: []core.Field{
				{Name: "id", Type: core.FieldTypeString},
				{Name: "value", Type: core.FieldTypeFloat, Default: 0.0},
				{Name: "region", Type: core.FieldTypeString},
				{Name: "currency", Type: core.FieldTypeString, Default: "USD"},
			},
		}
		if err := reg.Register(v2); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, "Schema evolved to v2 (zero downtime)\n")
	})

	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background()) //nolint:errcheck
	}()

	fmt.Println("Drift running. Endpoints:")
	fmt.Println("  GET  http://localhost:8080/metrics")
	fmt.Println("  GET  http://localhost:8080/debug/explain  (requires GEMINI_API_KEY)")
	fmt.Println("  POST http://localhost:8080/schema/evolve")
	fmt.Println("Press Ctrl+C to stop.")

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}

	if err := <-pipelineErr; err != nil && err != context.Canceled {
		log.Printf("pipeline error: %v", err)
	}
}
