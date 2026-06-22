// Command e2ebench is an end-to-end throughput demo: the same Filter(even)+Map(+1)
// workload over 5M records, fed "off the wire" as encoded frames that are DECODED
// in the hot path — so decoding counts toward throughput, like real ingestion.
//
// Three configurations:
//  1. JSON + row engine        — json.Unmarshal → map[string]any pipeline
//  2. binary + vectorized      — binary columnar decode → vectorized pipeline
//  3. parallel binary + vec    — N partition shards decoded concurrently → vectorized
//
// This shows what actually moves end-to-end throughput: replacing JSON with a
// binary columnar codec, processing columnar, and reading shards in parallel.
//
// Run:  go run ./cmd/e2ebench
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/source"
	"github.com/gribovan2005/drift/pkg/vector"
	"github.com/gribovan2005/drift/sdk"
)

const (
	n     = 5_000_000
	chunk = 4096
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU()) // dedicated / "beast": use the node
	nBatches := n / chunk
	total := nBatches * chunk
	ctx := context.Background()

	// ── Produce-side (offline, not timed): the same data as binary frames and as
	// JSON frames, modelling a binary-columnar topic vs a JSON topic. ──────────
	batches := vector.GenInt64("v", nBatches, chunk, func(i int) int64 { return int64(i) })
	binFrames := make([][]byte, nBatches)
	binBytes := 0
	for i, b := range batches {
		f, err := vector.EncodeBatch(b)
		if err != nil {
			panic(err)
		}
		binFrames[i] = f
		binBytes += len(f)
	}
	jsonFrames := make([][]byte, nBatches)
	jsonBytes := 0
	for i := 0; i < nBatches; i++ {
		rows := make([]map[string]any, chunk)
		for j := range rows {
			rows[j] = map[string]any{"v": i*chunk + j}
		}
		f, _ := json.Marshal(rows)
		jsonFrames[i] = f
		jsonBytes += len(f)
	}

	rate := func(d time.Duration) float64 { return float64(total) / d.Seconds() }
	human := func(eps float64) string { return fmt.Sprintf("%7.2f M/s", eps/1e6) }

	// 1) JSON + row engine ─────────────────────────────────────────────────────
	t0 := time.Now()
	err := sdk.New().
		From(&jsonRowSource{frames: jsonFrames}).
		Filter(func(r sdk.Record) bool { return int(r.Payload["v"].(float64))%2 == 0 }).
		Map(func(r sdk.Record) (sdk.Record, error) {
			r.Payload["v"] = r.Payload["v"].(float64) + 1
			return r, nil
		}).
		To(sdk.Discard()).
		Run(ctx)
	must(err)
	jsonRate := rate(time.Since(t0))

	// 2) binary + vectorized (single source) ──────────────────────────────────
	t0 = time.Now()
	err = sdk.New().
		From(vector.BinSource(binFrames)).
		Apply(vector.FilterInt64("v", func(x int64) bool { return x%2 == 0 })).
		Apply(vector.MapInt64("v", func(x int64) int64 { return x + 1 })).
		To(vector.Discard()).
		Run(ctx)
	must(err)
	binRate := rate(time.Since(t0))

	// 3) parallel binary + vectorized (shards = NumCPU) ───────────────────────
	shards := runtime.NumCPU()
	subs := make([]core.Source, shards)
	for s := range subs {
		subs[s] = vector.BinSource(shardOf(binFrames, s, shards))
	}
	t0 = time.Now()
	err = sdk.New().
		From(source.NewParallel(subs...)).
		Apply(vector.FilterInt64("v", func(x int64) bool { return x%2 == 0 })).
		Apply(vector.MapInt64("v", func(x int64) int64 { return x + 1 })).
		To(vector.Discard()).
		Run(ctx)
	must(err)
	parRate := rate(time.Since(t0))

	fmt.Printf("\nDrift end-to-end demo — Filter(even)+Map(+1), %d records, GOMAXPROCS=%d\n", total, runtime.NumCPU())
	fmt.Printf("wire size: json %d MB, binary %d MB\n\n", jsonBytes>>20, binBytes>>20)
	fmt.Printf("  %-34s %s  (1.00x)\n", "1) JSON + row (map[string]any)", human(jsonRate))
	fmt.Printf("  %-34s %s  (%.1fx)\n", "2) binary + vectorized", human(binRate), binRate/jsonRate)
	fmt.Printf("  %-34s %s  (%.1fx)\n", fmt.Sprintf("3) parallel(%d) binary + vec", shards), human(parRate), parRate/jsonRate)
	fmt.Println()
}

// jsonRowSource decodes JSON frames ([]map[string]any) in the read path and emits
// row records — modelling a JSON Kafka topic (decode cost counts).
type jsonRowSource struct{ frames [][]byte }

func (s *jsonRowSource) Read(ctx context.Context) (<-chan core.Record, error) {
	ch := make(chan core.Record, 256)
	go func() {
		defer close(ch)
		for _, f := range s.frames {
			var rows []map[string]any
			if json.Unmarshal(f, &rows) != nil {
				continue
			}
			for _, m := range rows {
				select {
				case ch <- core.Record{Payload: m}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return ch, nil
}

func shardOf(frames [][]byte, s, n int) [][]byte {
	var out [][]byte
	for i := s; i < len(frames); i += n {
		out = append(out, frames[i])
	}
	return out
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
