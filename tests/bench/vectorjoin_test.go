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

// rowEnrich is a row-path build-side join baseline (map lookup over map[string]any).
type rowEnrich struct {
	key, out string
	dim      map[int64]string
}

func (e *rowEnrich) Process(in []core.Record) ([]core.Record, error) {
	out := in[:0]
	for _, r := range in {
		v, ok := e.dim[int64(r.Payload[e.key].(int))]
		if !ok {
			continue
		}
		r.Payload[e.out] = v
		out = append(out, r)
	}
	return out, nil
}
func (e *rowEnrich) OnSchemaChange(core.Schema) {}

// TestVectorJoinThroughput compares columnar build-side hash join (enrichment)
// against the row-path equivalent.
//
//	go test ./tests/bench/ -run VectorJoin -v -count=1
func TestVectorJoinThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("join bench skipped in -short")
	}
	if raceEnabled {
		t.Skip("join bench skipped under -race (instrumentation distorts timing)")
	}

	const n, chunk, keys = 2_000_000, 4096, 1000
	rate := func(eps float64) string { return fmt.Sprintf("%7.2f M/s", eps/1e6) }

	// Dimension: keys → "c<k>".
	dimMap := make(map[int64]string, keys)
	dimIDs := make([]int64, keys)
	dimC := make([]string, keys)
	for k := 0; k < keys; k++ {
		dimIDs[k] = int64(k)
		dimC[k] = fmt.Sprintf("c%d", k)
		dimMap[int64(k)] = dimC[k]
	}

	// Row path.
	rows := make([]core.Record, n)
	for i := range rows {
		rows[i] = core.Record{Payload: map[string]any{"uid": i % keys, "amt": i}}
	}
	startRow := time.Now()
	err := sdk.New().From(source.NewMemory(rows)).
		Apply(&rowEnrich{key: "uid", out: "country", dim: dimMap}).
		To(sdk.Discard()).Run(context.Background())
	if err != nil {
		t.Fatalf("row run: %v", err)
	}
	rowRate := float64(n) / time.Since(startRow).Seconds()

	// Columnar path.
	dim := &core.Batch{
		Schema: core.Schema{Fields: []core.Field{
			{Name: "id", Type: core.FieldTypeInt},
			{Name: "country", Type: core.FieldTypeString},
		}},
		Len:  keys,
		Cols: []core.Column{{Kind: core.KindInt64, I64: dimIDs}, {Kind: core.KindString, Str: dimC}},
	}
	nB := n / chunk
	batches := make([]*core.Batch, nB)
	idx := 0
	for b := range batches {
		uid := make([]int64, chunk)
		amt := make([]int64, chunk)
		for j := range uid {
			uid[j] = int64(idx % keys)
			amt[j] = int64(idx)
			idx++
		}
		batches[b] = &core.Batch{
			Schema: core.Schema{Fields: []core.Field{{Name: "uid", Type: core.FieldTypeInt}, {Name: "amt", Type: core.FieldTypeInt}}},
			Len:    chunk,
			Cols:   []core.Column{{Kind: core.KindInt64, I64: uid}, {Kind: core.KindInt64, I64: amt}},
		}
	}
	startVec := time.Now()
	err = sdk.New().From(vector.MemSource(batches)).
		Apply(vector.HashJoin([]*core.Batch{dim}, "id", "uid").Bring("country", "country").Op()).
		To(vector.Discard()).Run(context.Background())
	if err != nil {
		t.Fatalf("vec run: %v", err)
	}
	vecRate := float64(nB*chunk) / time.Since(startVec).Seconds()

	t.Logf("── build-side hash join (enrich %d rows, %d-key dim) ──", n, keys)
	t.Logf("  row  (map[string]any):  %s  (1.00x)", rate(rowRate))
	t.Logf("  vec  (columnar):        %s  (%.1fx)", rate(vecRate), vecRate/rowRate)
}
