package bench

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/vector"
	"github.com/gribovan2005/drift/sdk"
)

// TestVectorVsRowThroughput runs the SAME logical workload — Filter(even) then
// Map(+1) over N int values — two ways: the map[string]any row engine vs the
// columnar vectorized fast-lane. It isolates the record-format cost (boxing + map +
// GC) that caps the row path.
//
// Run:  go test ./tests/bench/ -run VectorVsRow -v -count=1
func TestVectorVsRowThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("vector bench skipped in -short")
	}
	if raceEnabled {
		t.Skip("vector bench skipped under -race (instrumentation distorts timing)")
	}

	prev := runtime.GOMAXPROCS(1) // isolate per-record cost, single-threaded
	defer runtime.GOMAXPROCS(prev)

	const n = 5_000_000
	rate := func(eps float64) string { return fmt.Sprintf("%7.2f M/s", eps/1e6) }

	// ── Row path: map[string]any ────────────────────────────────────────────
	rows := makeBenchRecords(n) // Payload map[string]any{"v": i}
	startRow := time.Now()
	err := sdk.New().
		From(sdk.Slice(rows)).
		Filter(func(r sdk.Record) bool { return r.Payload["v"].(int)%2 == 0 }).
		Map(func(r sdk.Record) (sdk.Record, error) { r.Payload["v"] = r.Payload["v"].(int) + 1; return r, nil }).
		To(sdk.Discard()).
		Run(context.Background())
	if err != nil {
		t.Fatalf("row run: %v", err)
	}
	rowRate := float64(n) / time.Since(startRow).Seconds()

	// ── Vectorized path: columnar Int64 chunks ──────────────────────────────
	const chunk = 4096
	batches := vector.GenInt64("v", n/chunk, chunk, func(i int) int64 { return int64(i) })
	startVec := time.Now()
	err = sdk.New().
		From(vector.MemSource(batches)).
		Apply(vector.FilterInt64("v", func(x int64) bool { return x%2 == 0 })).
		Apply(vector.MapInt64("v", func(x int64) int64 { return x + 1 })).
		To(vector.Discard()).
		Run(context.Background())
	if err != nil {
		t.Fatalf("vec run: %v", err)
	}
	vecRate := float64((n/chunk)*chunk) / time.Since(startVec).Seconds()

	t.Logf("── Filter(even)+Map(+1), %d rows, GOMAXPROCS=1 ──", n)
	t.Logf("  row  (map[string]any):  %s  (1.00x)", rate(rowRate))
	t.Logf("  vec  (columnar Int64):  %s  (%.1fx)", rate(vecRate), vecRate/rowRate)
}
