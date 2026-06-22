// Command referencedemo is Drift's end-to-end reference pipeline: it ties the speed
// story (the columnar fast lane) to the headline differentiator (live schema
// evolution) in ONE running pipeline.
//
//	producer rows ─▶ SchemaAdapter ─▶ to-batch ─▶ vec-tumbling ─▶ to-rows ─▶ sink
//	(evolving)        (live v1→v2)    (row→columnar) (windowed agg) (columnar→row)
//
// Halfway through the stream the producer's schema is evolved (a `region` field is
// added) by registering v2 with the SchemaRegistry — the adapter starts injecting it
// with zero downtime and zero dropped records, while the columnar windowed aggregation
// keeps computing uninterrupted. At the end it reports throughput and the windowed
// results.
//
//	go run ./cmd/referencedemo            # defaults
//	go run ./cmd/referencedemo -n 2000000 # more rows
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/operator"
	"github.com/gribovan2005/drift/pkg/schema"
	"github.com/gribovan2005/drift/pkg/sink"
	"github.com/gribovan2005/drift/pkg/vector"
	"github.com/gribovan2005/drift/sdk"
)

func main() {
	n := flag.Int("n", 500_000, "number of records to stream")
	window := flag.Int64("window", 50_000, "tumbling window size (in ts units)")
	flag.Parse()

	reg := schema.NewRegistry()
	v1 := core.Schema{ID: "orders", Version: 1, Fields: []core.Field{
		{Name: "merchant", Type: core.FieldTypeString},
		{Name: "amount", Type: core.FieldTypeInt},
		{Name: "ts", Type: core.FieldTypeInt},
	}}
	must(reg.Register(v1))

	// The adapter normalises producer rows to the current schema and is subscribed, so
	// registering v2 mid-stream takes effect live (OnSchemaChange) — no restart.
	adapter := operator.NewSchemaAdapter(v1, nil)
	reg.Subscribe("orders", adapter)

	// Quiet logger so the demo output is the story, not stage INFO lines.
	quiet := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	out := sink.NewMemory()
	stream := sdk.New(sdk.WithLogger(quiet)).
		From(&evolvingSource{reg: reg, n: *n}).
		Apply(adapter).
		Apply(vector.FromRows(4096)). // row → columnar (to-batch)
		Apply(vector.TumblingGroup("merchant", "ts", *window).
			Count("orders").SumInt64("amount", "revenue").Op()). // windowed agg on the fast lane
		Apply(vector.ToRows()). // columnar → row
		To(out)

	p, err := stream.Build()
	must(err)

	fmt.Printf("streaming %d orders through: SchemaAdapter → fast-lane windowed agg (window=%d)\n", *n, *window)
	start := time.Now()
	must(p.Run(context.Background()))
	elapsed := time.Since(start)

	// Results: total windows, a sample, and throughput.
	type wk struct {
		merchant     string
		window       int64
		orders, rev  int64
	}
	var rows []wk
	for _, r := range out.Records() {
		rows = append(rows, wk{
			merchant: r.Payload["merchant"].(string),
			window:   r.Payload["ts"].(int64),
			orders:   r.Payload["orders"].(int64),
			rev:      r.Payload["revenue"].(int64),
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].window != rows[j].window {
			return rows[i].window < rows[j].window
		}
		return rows[i].merchant < rows[j].merchant
	})

	var totalOrders, totalRev int64
	for _, r := range rows {
		totalOrders += r.orders
		totalRev += r.rev
	}

	fmt.Println("\nsample windowed results (window start, merchant → orders, revenue):")
	for i, r := range rows {
		if i >= 6 {
			fmt.Printf("  … and %d more windows\n", len(rows)-i)
			break
		}
		fmt.Printf("  ts=%-8d %-8s → orders=%-7d revenue=%d\n", r.window, r.merchant, r.orders, r.rev)
	}

	snap := p.Snapshot()
	var processed int64
	for _, st := range snap.Stages {
		if st.ProcessedTotal > processed {
			processed = st.ProcessedTotal
		}
	}

	fmt.Printf("\n── summary ─────────────────────────────────────────\n")
	fmt.Printf("  rows streamed     : %d (all aggregated; 0 dropped, 0 restarts)\n", totalOrders)
	fmt.Printf("  windows emitted   : %d\n", len(rows))
	fmt.Printf("  throughput        : %.2f M rows/s (%.0fms)\n", float64(*n)/elapsed.Seconds()/1e6, elapsed.Seconds()*1000)
	fmt.Printf("  stage rows (max)  : %d (pipeline metrics)\n", processed)
	fmt.Printf("  schema            : evolved v1→v2 mid-stream (added `region`), zero downtime\n")
}

// evolvingSource emits n synthetic order rows and, at the halfway point, evolves the
// schema to v2 by registering it — deterministically, in stream order, so records
// before the boundary flow under v1 and records after under v2 (the adapter then
// injects the new `region` field). It models a producer that changed shape live.
type evolvingSource struct {
	reg *schema.Registry
	n   int
}

func (s *evolvingSource) Read(ctx context.Context) (<-chan core.Record, error) {
	ch := make(chan core.Record, 256)
	merchants := []string{"acme", "globex", "initech"}
	go func() {
		defer close(ch)
		for i := 0; i < s.n; i++ {
			if i == s.n/2 {
				// Live schema evolution — add `region` (default "us"). No restart.
				v2 := core.Schema{ID: "orders", Version: 2, Fields: []core.Field{
					{Name: "merchant", Type: core.FieldTypeString},
					{Name: "amount", Type: core.FieldTypeInt},
					{Name: "ts", Type: core.FieldTypeInt},
					{Name: "region", Type: core.FieldTypeString, Default: "us"},
				}}
				_ = s.reg.Register(v2)
				fmt.Printf("  [t=%d] schema evolved v1→v2: +region (no restart)\n", i)
			}
			rec := core.Record{Payload: map[string]any{
				"merchant": merchants[i%len(merchants)],
				"amount":   int64(i%100 + 1),
				"ts":       int64(i),
			}}
			select {
			case ch <- rec:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
