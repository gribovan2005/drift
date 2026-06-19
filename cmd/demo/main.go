// demo runs a payment-processing pipeline with live schema evolution and
// exposes the Drift Web UI on :8080.
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
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
	"github.com/andrejgribov/drift/pkg/web"
)

var (
	merchants = []string{"stripe", "paypal", "adyen", "braintree", "square"}
	currencies = []string{"USD", "EUR", "GBP", "JPY", "BRL"}
	regions    = []string{"us-east", "eu-west", "ap-south", "sa-east", "af-south"}
)

func main() {
	dotenv.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := schema.NewRegistry()

	// v1: basic payment event
	v1 := core.Schema{
		ID:      "payment",
		Version: 1,
		Fields: []core.Field{
			{Name: "tx_id",    Type: core.FieldTypeString},
			{Name: "merchant", Type: core.FieldTypeString},
			{Name: "amount",   Type: core.FieldTypeFloat},
			{Name: "currency", Type: core.FieldTypeString},
			{Name: "region",   Type: core.FieldTypeString},
		},
	}
	if err := reg.Register(v1); err != nil {
		log.Fatal(err)
	}

	adapter := operator.NewSchemaAdapter(v1, operator.AliasMap{})
	reg.Subscribe("payment", adapter)

	// Window: aggregate per 50 transactions
	window, _ := operator.NewTumblingWindow(50, func(records []core.Record) (core.Record, error) {
		totalAmount := 0.0
		for _, r := range records {
			totalAmount += r.Payload["amount"].(float64)
		}
		return core.Record{
			Payload: map[string]any{
				"window_size":   len(records),
				"total_amount":  totalAmount,
				"avg_amount":    totalAmount / float64(len(records)),
			},
		}, nil
	})

	stages := []pipeline.Stage{
		{
			Label: "fraud-filter",
			Op: operator.NewFilter(func(r core.Record) bool {
				// Flag suspiciously large transactions (>9500) — reject them.
				return r.Payload["amount"].(float64) <= 9500.0
			}),
		},
		{
			Label: "schema-adapt",
			Op:    adapter,
		},
		{
			Label: "enrich",
			Op: operator.NewMap(func(r core.Record) (core.Record, error) {
				r.Payload["processed"] = true
				return r, nil
			}),
		},
		{
			Label: "aggregator",
			Op:    window,
		},
	}

	rng := rand.New(rand.NewSource(42))
	src := source.NewGenerator(func(seq int) core.Record {
		amount := rng.Float64()*10000 + 0.5
		// Inject occasional spikes to make the fraud-filter interesting.
		if seq%17 == 0 {
			amount = 9800 + rng.Float64()*500
		}
		return core.Record{
			SchemaID:      "payment",
			SchemaVersion: 1,
			Payload: map[string]any{
				"tx_id":    fmt.Sprintf("tx-%08d", seq),
				"merchant": merchants[seq%len(merchants)],
				"amount":   amount,
				"currency": currencies[rng.Intn(len(currencies))],
				"region":   regions[seq%len(regions)],
			},
		}
	}, 2*time.Millisecond) // ~500 records/sec

	snk := sink.NewMemory()
	p := pipeline.New(src, stages, snk)

	dbg := ai.New("", "")

	// Schedule schema evolution at t+30s to demo live evolution.
	go func() {
		select {
		case <-time.After(30 * time.Second):
			v2 := core.Schema{
				ID:      "payment",
				Version: 2,
				Fields: []core.Field{
					{Name: "tx_id",      Type: core.FieldTypeString},
					{Name: "merchant",   Type: core.FieldTypeString},
					{Name: "amount",     Type: core.FieldTypeFloat},
					{Name: "currency",   Type: core.FieldTypeString},
					{Name: "region",     Type: core.FieldTypeString},
					{Name: "risk_score", Type: core.FieldTypeFloat,  Default: 0.0},  // new field
					{Name: "verified",   Type: core.FieldTypeBool,   Default: false}, // new field
				},
			}
			if err := reg.Register(v2); err != nil {
				log.Printf("schema v2: %v", err)
				return
			}
			log.Println("✓ Schema evolved to v2 (risk_score + verified added — zero downtime)")
		case <-ctx.Done():
		}
	}()

	// Pipeline in background.
	go func() {
		if err := p.Run(ctx); err != nil && err != context.Canceled {
			log.Printf("pipeline: %v", err)
		}
	}()

	srv := web.New(":8080", p, reg, dbg)

	fmt.Println("╔══════════════════════════════════════════════╗")
	fmt.Println("║       Drift Demo — Payment Pipeline          ║")
	fmt.Println("╠══════════════════════════════════════════════╣")
	fmt.Println("║  UI      →  http://localhost:8080            ║")
	fmt.Println("║  Schema v2 evolution in 30 seconds           ║")
	fmt.Println("║  AI debug requires GEMINI_API_KEY env var    ║")
	fmt.Println("╚══════════════════════════════════════════════╝")

	if err := srv.ListenAndServe(ctx); err != nil {
		log.Printf("web server: %v", err)
	}
}
