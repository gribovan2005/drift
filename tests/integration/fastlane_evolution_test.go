package integration

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/operator"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/schema"
	"github.com/gribovan2005/drift/pkg/sink"
	"github.com/gribovan2005/drift/pkg/vector"
	"github.com/stretchr/testify/require"
)

// evolvingSrc emits n order rows and registers schema v2 at the halfway point (in
// stream order), modelling a producer that changes shape live. Mirrors cmd/referencedemo.
type evolvingSrc struct {
	reg *schema.Registry
	n   int
}

func (s *evolvingSrc) Read(ctx context.Context) (<-chan core.Record, error) {
	ch := make(chan core.Record, 128)
	go func() {
		defer close(ch)
		for i := 0; i < s.n; i++ {
			if i == s.n/2 {
				_ = s.reg.Register(core.Schema{ID: "orders", Version: 2, Fields: []core.Field{
					{Name: "merchant", Type: core.FieldTypeString},
					{Name: "amount", Type: core.FieldTypeInt},
					{Name: "ts", Type: core.FieldTypeInt},
					{Name: "region", Type: core.FieldTypeString, Default: "us"},
				}})
			}
			select {
			case ch <- core.Record{Payload: map[string]any{"merchant": "acme", "amount": int64(2), "ts": int64(i)}}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

// TestFastLane_EvolvesAcrossSchemaBoundary proves the reference stack composes: an
// evolving row stream → SchemaAdapter (live v1→v2) → fast-lane windowed aggregation →
// rows, with every record aggregated across the schema change (zero drops, no restart).
func TestFastLane_EvolvesAcrossSchemaBoundary(t *testing.T) {
	const n = 10_000
	const window = 2_500

	reg := schema.NewRegistry()
	v1 := core.Schema{ID: "orders", Version: 1, Fields: []core.Field{
		{Name: "merchant", Type: core.FieldTypeString},
		{Name: "amount", Type: core.FieldTypeInt},
		{Name: "ts", Type: core.FieldTypeInt},
	}}
	require.NoError(t, reg.Register(v1))
	adapter := operator.NewSchemaAdapter(v1, nil)
	reg.Subscribe("orders", adapter)

	out := sink.NewMemory()
	p := pipeline.New(
		&evolvingSrc{reg: reg, n: n},
		[]pipeline.Stage{
			{Label: "adapt", Op: adapter},
			{Label: "batch", Op: vector.FromRows(1024)},
			{Label: "win", Op: vector.TumblingGroup("merchant", "ts", window).Count("orders").SumInt64("amount", "revenue").Op()},
			{Label: "rows", Op: vector.ToRows()},
		},
		out,
	)
	require.NoError(t, p.Run(context.Background()))

	var totalOrders, totalRevenue int64
	for _, r := range out.Records() {
		totalOrders += r.Payload["orders"].(int64)
		totalRevenue += r.Payload["revenue"].(int64)
	}
	// Every one of the n rows must be counted across the schema boundary, amount=2 each.
	require.Equal(t, int64(n), totalOrders, "all rows aggregated across the schema change")
	require.Equal(t, int64(2*n), totalRevenue)
}
