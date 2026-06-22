// Command sdkdemo shows what the Drift SDK is FOR: embedding a real-time stream
// processing pipeline directly inside an ordinary Go HTTP service — no Flink, no
// cluster, no JVM. One file builds a payment-analytics pipeline with the fluent
// SDK and serves its live results over HTTP.
//
// It demonstrates, in one ~150-line service, the things you'd otherwise stand up
// a streaming cluster for:
//
//   - a fluent pipeline:   Generate → Filter (fraud guard) → SchemaAdapter →
//     enrich → Tumbling window aggregate → custom sink
//   - a real-time materialized view readable from the service at GET /stats
//     (per-window totals updated in-process, no database)
//   - LIVE SCHEMA EVOLUTION: at t+15s a v2 schema adds risk_score/verified and
//     the running pipeline adapts with zero downtime — watch /stats flip
//     "schema_has_risk_score" to true
//   - Prometheus metrics at GET /metrics via sdk.PrometheusHandler
//
// Run it:
//
//	go run ./cmd/sdkdemo
//	curl localhost:8090/stats
//	curl localhost:8090/metrics
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gribovan2005/drift/pkg/operator"
	"github.com/gribovan2005/drift/pkg/schema"
	"github.com/gribovan2005/drift/sdk"
)

const addr = ":8090"

var merchants = []string{"stripe", "paypal", "adyen", "braintree", "square"}

func main() {
	profileName := flag.String("profile", "sidecar", "resource profile: sidecar|dedicated")
	flag.Parse()

	// The demo owns its process, so the Dedicated profile may tune process-global
	// runtime knobs (GOMAXPROCS/GOGC) via OwnsProcess().
	var profile sdk.Profile
	switch *profileName {
	case "dedicated":
		profile = sdk.Dedicated.OwnsProcess()
	case "sidecar":
		profile = sdk.Sidecar.OwnsProcess()
	default:
		log.Fatalf("unknown -profile %q (want sidecar|dedicated)", *profileName)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── Schema registry: v1 today, v2 published live at t+15s ────────────────
	reg := schema.NewRegistry()
	v1 := sdk.Schema{
		ID: "payment", Version: 1,
		Fields: []sdk.Field{
			{Name: "tx_id", Type: sdk.String},
			{Name: "merchant", Type: sdk.String},
			{Name: "amount", Type: sdk.Float},
		},
	}
	if err := reg.Register(v1); err != nil {
		log.Fatal(err)
	}
	// The SchemaAdapter is a normal core.Operator → it goes in via Apply, and it
	// subscribes to the registry so OnSchemaChange fires when v2 is published.
	adapter := operator.NewSchemaAdapter(v1, operator.AliasMap{})
	reg.Subscribe("payment", adapter)

	// ── The materialized view, maintained by a tiny custom sink ──────────────
	view := &liveView{}

	// ── Build the pipeline with the fluent SDK ───────────────────────────────
	rng := rand.New(rand.NewSource(42))
	p, err := sdk.New(sdk.WithProfile(profile)).
		From(sdk.Generate(func(seq int) sdk.Record {
			amount := rng.Float64()*10000 + 0.5
			if seq%17 == 0 { // occasional spike for the fraud guard to drop
				amount = 9800 + rng.Float64()*500
			}
			return sdk.Record{
				SchemaID: "payment", SchemaVersion: 1,
				Payload: map[string]any{
					"tx_id":    fmt.Sprintf("tx-%08d", seq),
					"merchant": merchants[seq%len(merchants)],
					"amount":   amount,
				},
			}
		}, 2*time.Millisecond)). // ~500 tx/sec
		Filter(func(r sdk.Record) bool { return r.Payload["amount"].(float64) <= 9500 }).
		ApplyLabeled("schema-adapt", adapter).
		Map(func(r sdk.Record) (sdk.Record, error) {
			r.Payload["processed"] = true
			return r, nil
		}).
		Tumbling(50, func(w []sdk.Record) (sdk.Record, error) {
			total, hasRisk := 0.0, false
			for _, r := range w {
				total += r.Payload["amount"].(float64)
				if _, ok := r.Payload["risk_score"]; ok {
					hasRisk = true // appears only after the v2 schema lands
				}
			}
			return sdk.Record{Payload: map[string]any{
				"window_size":           len(w),
				"total_amount":          total,
				"avg_amount":            total / float64(len(w)),
				"schema_has_risk_score": hasRisk,
			}}, nil
		}).
		To(view).
		Build()
	if err != nil {
		log.Fatal(err)
	}

	// ── Run the pipeline in the background; serve its results over HTTP ───────
	go func() {
		if err := p.Run(ctx); err != nil && err != context.Canceled {
			log.Printf("pipeline: %v", err)
		}
	}()

	// Live schema evolution at t+15s — zero downtime.
	go func() {
		select {
		case <-time.After(15 * time.Second):
			v2 := sdk.Schema{
				ID: "payment", Version: 2,
				Fields: append(append([]sdk.Field{}, v1.Fields...),
					sdk.Field{Name: "risk_score", Type: sdk.Float, Default: 0.0},
					sdk.Field{Name: "verified", Type: sdk.Bool, Default: false},
				),
			}
			if err := reg.Register(v2); err != nil {
				log.Printf("schema v2: %v", err)
				return
			}
			log.Println("✓ schema evolved to v2 (risk_score + verified) — zero downtime")
		case <-ctx.Done():
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(view.snapshot())
	})
	mux.Handle("GET /metrics", sdk.PrometheusHandler(p)) // the exporter we just built

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() { <-ctx.Done(); _ = srv.Shutdown(context.Background()) }()

	fmt.Printf(`
Drift SDK demo — real-time payment analytics embedded in a Go service
  profile: %s (batch=%d, buffer=%d, linger=%s, GOMAXPROCS=%d)
  GET http://localhost%s/stats     live window aggregates (materialized view)
  GET http://localhost%s/metrics   Prometheus scrape
  → live schema evolution at t+15s (watch "schema_has_risk_score" flip to true)
`, *profileName, profile.BatchSize, profile.ChannelBuffer, profile.MaxLinger, profile.GOMAXPROCS, addr, addr)

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("http: %v", err)
	}
}

// liveView is a custom sink that keeps the latest window aggregate plus running
// totals — a materialized analytical entity readable straight from the service,
// no database. It implements core.Sink (sdk.Sink).
type liveView struct {
	mu          sync.Mutex
	windows     int
	totalAmount float64
	latest      map[string]any
}

func (v *liveView) Write(ctx context.Context, ch <-chan sdk.Record) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r, ok := <-ch:
			if !ok {
				return nil
			}
			v.mu.Lock()
			v.windows++
			v.totalAmount += r.Payload["total_amount"].(float64)
			v.latest = r.Payload
			v.mu.Unlock()
		}
	}
}

func (v *liveView) snapshot() map[string]any {
	v.mu.Lock()
	defer v.mu.Unlock()
	return map[string]any{
		"windows_aggregated":  v.windows,
		"total_amount_so_far": v.totalAmount,
		"latest_window":       v.latest,
	}
}
