package bench

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/source"
	"github.com/gribovan2005/drift/pkg/vector"
	"github.com/gribovan2005/drift/sdk"
)

// rowGroup is a row-path global group-by baseline (map[string]any per record),
// for comparison against the columnar vector.GroupBy.
type rowGroup struct {
	key, val string
	m        map[string]*[2]float64 // [count, sum]
}

func (g *rowGroup) Process(in []core.Record) ([]core.Record, error) {
	for _, r := range in {
		k := r.Payload[g.key].(string)
		a := g.m[k]
		if a == nil {
			a = &[2]float64{}
			g.m[k] = a
		}
		a[0]++
		a[1] += r.Payload[g.val].(float64)
	}
	return nil, nil
}
func (g *rowGroup) OnSchemaChange(core.Schema)    {}
func (g *rowGroup) Flush() ([]core.Record, error) { return nil, nil }

// TestVectorGroupByThroughput compares columnar GROUP BY against the row-path
// equivalent on the same count+sum-by-key workload.
//
//	go test ./tests/bench/ -run VectorGroupBy -v -count=1
func TestVectorGroupByThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("group-by bench skipped in -short")
	}
	if raceEnabled {
		t.Skip("group-by bench skipped under -race (instrumentation distorts timing)")
	}

	const n, chunk, keys = 2_000_000, 4096, 1000
	rate := func(eps float64) string { return fmt.Sprintf("%7.2f M/s", eps/1e6) }

	// Row path: map[string]any records.
	rows := make([]core.Record, n)
	for i := range rows {
		rows[i] = core.Record{Payload: map[string]any{
			"cat": fmt.Sprintf("k%d", i%keys),
			"amt": float64(i % 100),
		}}
	}
	startRow := time.Now()
	err := sdk.New().From(source.NewMemory(rows)).
		Apply(&rowGroup{key: "cat", val: "amt", m: make(map[string]*[2]float64)}).
		To(sdk.Discard()).Run(context.Background())
	if err != nil {
		t.Fatalf("row run: %v", err)
	}
	rowRate := float64(n) / time.Since(startRow).Seconds()

	// Columnar path: string key + float column chunks.
	nB := n / chunk
	batches := make([]*core.Batch, nB)
	idx := 0
	for b := range batches {
		cats := make([]string, chunk)
		amts := make([]float64, chunk)
		for j := range cats {
			cats[j] = fmt.Sprintf("k%d", idx%keys)
			amts[j] = float64(idx % 100)
			idx++
		}
		batches[b] = &core.Batch{
			Schema: core.Schema{Fields: []core.Field{
				{Name: "cat", Type: core.FieldTypeString},
				{Name: "amt", Type: core.FieldTypeFloat},
			}},
			Len:  chunk,
			Cols: []core.Column{{Kind: core.KindString, Str: cats}, {Kind: core.KindFloat64, F64: amts}},
		}
	}
	startVec := time.Now()
	err = sdk.New().From(vector.MemSource(batches)).
		Apply(vector.GroupBy("cat").Count("n").SumFloat64("amt", "total").Op()).
		To(vector.Discard()).Run(context.Background())
	if err != nil {
		t.Fatalf("vec run: %v", err)
	}
	vecRate := float64(nB*chunk) / time.Since(startVec).Seconds()

	t.Logf("── GROUP BY cat → count, sum(amt); %d rows, %d keys ──", n, keys)
	t.Logf("  row  (map[string]any):  %s  (1.00x)", rate(rowRate))
	t.Logf("  vec  (columnar):        %s  (%.1fx)", rate(vecRate), vecRate/rowRate)
}
