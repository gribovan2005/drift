// Command schemademo shows Drift's headline feature — live schema evolution with
// zero downtime. The SAME raw producer records are processed under schema v1, then the
// schema is evolved to v2 (a field renamed, a field added, and a column's TYPE widened
// int→float) by registering the new version with the SchemaRegistry — which pushes it
// to the subscribed SchemaAdapter via OnSchemaChange. No restart, no state migration:
// the next records simply conform to v2.
//
//	go run ./cmd/schemademo
package main

import (
	"fmt"
	"sort"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/operator"
	"github.com/gribovan2005/drift/pkg/schema"
)

func main() {
	reg := schema.NewRegistry()

	// v1: amount is an int (cents); no region.
	v1 := core.Schema{ID: "payments", Version: 1, Fields: []core.Field{
		{Name: "merchant", Type: core.FieldTypeString},
		{Name: "amount", Type: core.FieldTypeInt},
	}}
	must(reg.Register(v1))

	// The adapter normalises producer records to the current schema. It is subscribed,
	// so registering a new version pushes it here live. The alias records that v2's
	// "amount_usd" is the old "amount".
	adapter := operator.NewSchemaAdapter(v1, operator.AliasMap{"amount": "amount_usd"})
	reg.Subscribe("payments", adapter)

	// Raw records as the producer emits them (old shape) — unchanged across the demo.
	raw := []core.Record{
		{Payload: map[string]any{"merchant": "acme", "amount": int64(1299)}},
		{Payload: map[string]any{"merchant": "globex", "amount": int64(50)}},
	}

	fmt.Println("── v1 (amount: int) ────────────────────────────────")
	emit(adapter, raw)

	// Evolve the schema at runtime. Register pushes v2 to every subscriber's
	// OnSchemaChange synchronously — the running pipeline never stops.
	v2 := core.Schema{ID: "payments", Version: 2, Fields: []core.Field{
		{Name: "merchant", Type: core.FieldTypeString},
		{Name: "amount_usd", Type: core.FieldTypeFloat},          // renamed from amount + retyped int→float
		{Name: "region", Type: core.FieldTypeString, Default: "us"}, // new field with a default
	}}
	must(reg.Register(v2))

	fmt.Println("\n── v2 (live: amount→amount_usd, int→float, +region) — no restart ──")
	emit(adapter, raw) // SAME raw input, now conforming to v2
}

func emit(adapter *operator.SchemaAdapter, raw []core.Record) {
	out, err := adapter.Process(raw)
	must(err)
	for _, r := range out {
		// Print each field as name=value(GoType) so the int→float retype is visible
		// (JSON would render 1299.0 as "1299").
		keys := make([]string, 0, len(r.Payload))
		for k := range r.Payload {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Printf("  v%d ", r.SchemaVersion)
		for _, k := range keys {
			fmt.Printf(" %s=%v(%T)", k, r.Payload[k], r.Payload[k])
		}
		fmt.Println()
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
