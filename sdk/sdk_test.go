package sdk_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gribovan2005/drift/sdk"
	"github.com/gribovan2005/drift/pkg/checkpoint"
	"github.com/gribovan2005/drift/pkg/lineage"
)

// recs builds n records with an int field "v" = 0..n-1.
func recs(n int) []sdk.Record {
	out := make([]sdk.Record, n)
	for i := range out {
		out[i] = sdk.Record{Payload: map[string]any{"v": i}}
	}
	return out
}

// ─── Builder behaviour ────────────────────────────────────────────────────

func TestMap_Filter_Collect(t *testing.T) {
	var out []sdk.Record
	err := sdk.New().
		From(sdk.Slice(recs(6))).
		Filter(func(r sdk.Record) bool { return r.Payload["v"].(int)%2 == 0 }).
		Map(func(r sdk.Record) (sdk.Record, error) {
			r.Payload["v"] = r.Payload["v"].(int) + 1
			return r, nil
		}).
		To(sdk.CollectInto(&out)).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	got := values(out)
	sort.Ints(got)
	if want := []int{1, 3, 5}; !equal(got, want) { // evens 0,2,4 → +1
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestFlatMap(t *testing.T) {
	c := sdk.Collect()
	err := sdk.New().
		From(sdk.Slice(recs(3))).
		FlatMap(func(r sdk.Record) ([]sdk.Record, error) {
			return []sdk.Record{r, r}, nil // duplicate each
		}).
		To(c).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(c.Records()) != 6 {
		t.Fatalf("got %d records, want 6", len(c.Records()))
	}
}

func TestTumbling(t *testing.T) {
	var out []sdk.Record
	err := sdk.New().
		From(sdk.Slice(recs(6))).
		Tumbling(3, func(w []sdk.Record) (sdk.Record, error) {
			sum := 0
			for _, r := range w {
				sum += r.Payload["v"].(int)
			}
			return sdk.Record{Payload: map[string]any{"sum": sum}}, nil
		}).
		To(sdk.CollectInto(&out)).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("got %d windows, want 2", len(out))
	}
	sums := []int{out[0].Payload["sum"].(int), out[1].Payload["sum"].(int)}
	sort.Ints(sums)
	if !equal(sums, []int{3, 12}) { // 0+1+2, 3+4+5
		t.Fatalf("got sums %v, want [3 12]", sums)
	}
}

func TestSliding(t *testing.T) {
	c := sdk.Collect()
	err := sdk.New().
		From(sdk.Slice(recs(5))).
		Sliding(3, 1, func(w []sdk.Record) (sdk.Record, error) {
			return sdk.Record{Payload: map[string]any{"n": len(w)}}, nil
		}).
		To(c).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	// step 1 → one emission per input record = 5 (windows grow 1,2,3 then slide)
	if len(c.Records()) != 5 {
		t.Fatalf("got %d windows, want 5", len(c.Records()))
	}
}

func TestDeduplicate(t *testing.T) {
	in := []sdk.Record{
		{Payload: map[string]any{"k": "a"}},
		{Payload: map[string]any{"k": "a"}},
		{Payload: map[string]any{"k": "b"}},
		{Payload: map[string]any{"k": "b"}},
		{Payload: map[string]any{"k": "c"}},
	}
	var out []sdk.Record
	err := sdk.New().
		From(sdk.Slice(in)).
		Deduplicate(func(r sdk.Record) string { return r.Payload["k"].(string) }, time.Hour).
		To(sdk.CollectInto(&out)).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("got %d unique, want 3", len(out))
	}
}

// addField is a custom core.Operator used to test the Apply escape hatch.
type addField struct{ key string }

func (a addField) Process(in []sdk.Record) ([]sdk.Record, error) {
	for i := range in {
		in[i].Payload[a.key] = true
	}
	return in, nil
}
func (addField) OnSchemaChange(sdk.Schema) {}

func TestApply_EscapeHatch(t *testing.T) {
	var out []sdk.Record
	err := sdk.New().
		From(sdk.Slice(recs(2))).
		Apply(addField{key: "flagged"}).
		To(sdk.CollectInto(&out)).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, r := range out {
		if r.Payload["flagged"] != true {
			t.Fatalf("custom operator did not run: %+v", r.Payload)
		}
	}
}

func TestAutoLabels_AllKinds(t *testing.T) {
	p, err := sdk.New().
		From(sdk.Slice(recs(1))).
		Filter(func(sdk.Record) bool { return true }).
		Map(func(r sdk.Record) (sdk.Record, error) { return r, nil }).
		FlatMap(func(r sdk.Record) ([]sdk.Record, error) { return []sdk.Record{r}, nil }).
		Tumbling(1, func(w []sdk.Record) (sdk.Record, error) { return w[0], nil }).
		Deduplicate(func(sdk.Record) string { return "k" }, time.Hour).
		Apply(addField{key: "x"}).
		ApplyLabeled("custom", addField{key: "y"}).
		To(sdk.Discard()).
		Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	var labels []string
	for _, n := range p.Graph() {
		labels = append(labels, n.Label)
	}
	// Global 1-based index across kinds; ApplyLabeled overrides.
	want := []string{"filter-1", "map-2", "flatmap-3", "window-4", "dedup-5", "op-6", "custom"}
	if !equalStr(labels, want) {
		t.Fatalf("got labels %v, want %v", labels, want)
	}
}

func TestPassthrough_ZeroStages(t *testing.T) {
	c := sdk.Collect()
	p, err := sdk.New().From(sdk.Slice(recs(4))).To(c).Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	// A stage-less stream must inject an identity "passthrough" so records flow.
	g := p.Graph()
	if len(g) != 1 || g[0].Label != "passthrough" {
		t.Fatalf("expected single passthrough stage, got %+v", g)
	}
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(c.Records()) != 4 {
		t.Fatalf("got %d, want 4", len(c.Records()))
	}
}

func TestBuild_ForMonitoring(t *testing.T) {
	p, err := sdk.New().
		From(sdk.Slice(recs(1))).
		Filter(func(sdk.Record) bool { return true }).
		To(sdk.Discard()).
		Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(p.Graph()) == 0 {
		t.Fatal("expected non-empty graph")
	}
}

// ─── Error handling ───────────────────────────────────────────────────────

func TestMissingSource_Errors(t *testing.T) {
	if err := sdk.New().To(sdk.Discard()).Run(context.Background()); err == nil {
		t.Fatal("expected error for missing source")
	}
	if _, err := sdk.New().To(sdk.Discard()).Build(); err == nil {
		t.Fatal("expected Build error for missing source")
	}
}

func TestMissingSink_Errors(t *testing.T) {
	if err := sdk.New().From(sdk.Slice(recs(1))).Run(context.Background()); err == nil {
		t.Fatal("expected error for missing sink")
	}
}

func TestBuilderError_Tumbling(t *testing.T) {
	err := sdk.New().
		From(sdk.Slice(recs(3))).
		Tumbling(0, func([]sdk.Record) (sdk.Record, error) { return sdk.Record{}, nil }).
		To(sdk.Discard()).
		Run(context.Background())
	if err == nil {
		t.Fatal("expected error for invalid tumbling window size")
	}
}

func TestBuilderError_Sliding(t *testing.T) {
	// size < step is invalid.
	_, err := sdk.New().
		From(sdk.Slice(recs(3))).
		Sliding(2, 5, func([]sdk.Record) (sdk.Record, error) { return sdk.Record{}, nil }).
		To(sdk.Discard()).
		Build()
	if err == nil {
		t.Fatal("expected error for size < step")
	}
}

func TestBuilderError_ShortCircuits(t *testing.T) {
	// After a builder error, later stage methods must be no-ops and the FIRST
	// error wins — even though we keep chaining.
	s := sdk.New().
		From(sdk.Slice(recs(3))).
		Tumbling(0, func([]sdk.Record) (sdk.Record, error) { return sdk.Record{}, nil }). // error here
		Map(func(r sdk.Record) (sdk.Record, error) { return r, nil }).                       // ignored
		To(sdk.Discard())
	_, err := s.Build()
	if err == nil {
		t.Fatal("expected first builder error to survive later chaining")
	}
}

func TestRuntimeError_Map(t *testing.T) {
	sentinel := errors.New("boom")
	err := sdk.New().
		From(sdk.Slice(recs(3))).
		Map(func(r sdk.Record) (sdk.Record, error) { return r, sentinel }).
		To(sdk.Discard()).
		Run(context.Background())
	if err == nil {
		t.Fatal("expected map runtime error to surface from Run")
	}
}

func TestRuntimeError_FlatMap(t *testing.T) {
	err := sdk.New().
		From(sdk.Slice(recs(3))).
		FlatMap(func(r sdk.Record) ([]sdk.Record, error) { return nil, errors.New("boom") }).
		To(sdk.Discard()).
		Run(context.Background())
	if err == nil {
		t.Fatal("expected flatmap runtime error to surface from Run")
	}
}

// ─── From/To overwrite semantics ──────────────────────────────────────────

func TestFrom_LastWins(t *testing.T) {
	c := sdk.Collect()
	err := sdk.New().
		From(sdk.Slice(recs(2))). // overwritten
		From(sdk.Slice(recs(5))).
		To(c).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(c.Records()) != 5 {
		t.Fatalf("got %d, want 5 (second From should win)", len(c.Records()))
	}
}

func TestTo_LastWins(t *testing.T) {
	first := sdk.Collect()
	second := sdk.Collect()
	err := sdk.New().
		From(sdk.Slice(recs(3))).
		To(first). // overwritten
		To(second).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(first.Records()) != 0 {
		t.Fatalf("first sink should receive nothing, got %d", len(first.Records()))
	}
	if len(second.Records()) != 3 {
		t.Fatalf("second sink got %d, want 3", len(second.Records()))
	}
}

// ─── Options ──────────────────────────────────────────────────────────────

func TestWithLineage_MintsIDs(t *testing.T) {
	tr := lineage.New()
	c := sdk.Collect()
	err := sdk.New(sdk.WithLineage(tr)).
		From(sdk.Slice(recs(3))).
		Map(func(r sdk.Record) (sdk.Record, error) { return r, nil }).
		To(c).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if tr.Len() == 0 {
		t.Fatal("lineage tracker empty — WithLineage not threaded through")
	}
	for _, r := range c.Records() {
		if r.ID == "" {
			t.Fatalf("record missing lineage ID: %+v", r)
		}
	}
}

func TestWithLogger_Threaded(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	err := sdk.New(sdk.WithLogger(logger)).
		From(sdk.Slice(recs(1))).
		To(sdk.Discard()).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !bytes.Contains(buf.Bytes(), []byte("pipeline")) {
		t.Fatalf("custom logger captured no pipeline logs:\n%s", buf.String())
	}
}

func TestWithCheckpoint_Threaded(t *testing.T) {
	dir := t.TempDir()
	store, err := checkpoint.NewFileStore(dir)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	// A stateful window is Snapshottable, so a checkpoint is written on stop.
	err = sdk.New(sdk.WithCheckpoint(store)).
		From(sdk.Slice(recs(4))).
		Tumbling(2, func(w []sdk.Record) (sdk.Record, error) { return w[0], nil }).
		To(sdk.Discard()).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) == 0 {
		t.Fatal("expected checkpoint files written — WithCheckpoint not threaded through")
	}
}

// ─── Source helpers ───────────────────────────────────────────────────────

func TestGenerate_RespectsContextCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	// The contract: an infinite source must STOP when ctx expires rather than
	// hang. (Whether the stage batcher has flushed buffered records to the sink
	// by cancel time is timing-dependent and not what we assert here.)
	done := make(chan error, 1)
	go func() {
		done <- sdk.New().
			From(sdk.Generate(func(seq int) sdk.Record {
				return sdk.Record{Payload: map[string]any{"seq": seq}}
			}, time.Millisecond)).
			To(sdk.Discard()).
			Run(ctx)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Generate stream did not stop on context cancel")
	}
}

// ─── Sink helpers ─────────────────────────────────────────────────────────

func TestDiscard_DropsAll(t *testing.T) {
	// Discard must drain without error and keep nothing observable.
	err := sdk.New().From(sdk.Slice(recs(100))).To(sdk.Discard()).Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestCollectInto_AppendsInOrder(t *testing.T) {
	var out []sdk.Record
	err := sdk.New().From(sdk.Slice(recs(4))).To(sdk.CollectInto(&out)).Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	got := values(out)
	if !equal(got, []int{0, 1, 2, 3}) { // single source goroutine ⇒ order preserved
		t.Fatalf("got %v, want [0 1 2 3]", got)
	}
}

func TestCollector_RecordsIsCopy(t *testing.T) {
	c := sdk.Collect()
	if err := sdk.New().From(sdk.Slice(recs(2))).To(c).Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	a := c.Records()
	if len(a) != 2 {
		t.Fatalf("got %d, want 2", len(a))
	}
	a[0] = sdk.Record{} // mutating the returned slice must not affect the sink
	if len(c.Records()) != 2 || c.Records()[0].Payload == nil {
		t.Fatal("Records() did not return an independent copy")
	}
}

func TestToFile_WritesNDJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.ndjson")
	err := sdk.New().From(sdk.Slice(recs(3))).To(sdk.ToFile(path)).Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	var lines int
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var rec map[string]any
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("line %d not valid JSON: %v", lines, err)
		}
		lines++
	}
	if lines != 3 {
		t.Fatalf("got %d NDJSON lines, want 3", lines)
	}
}

func TestHTTPSink_PostsRecords(t *testing.T) {
	var got int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&got, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	err := sdk.New().From(sdk.Slice(recs(5))).To(sdk.HTTPSink(srv.URL)).Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if n := atomic.LoadInt32(&got); n != 5 {
		t.Fatalf("server received %d posts, want 5", n)
	}
}

func TestHTTPSource_Constructs(t *testing.T) {
	// The deep behaviour is covered in pkg/source; here we assert the facade
	// helper yields a usable, non-nil Source.
	if sdk.HTTPSource(":0") == nil {
		t.Fatal("HTTPSource returned nil")
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────

func values(rs []sdk.Record) []int {
	out := make([]int, len(rs))
	for i, r := range rs {
		out[i] = r.Payload["v"].(int)
	}
	return out
}

func equal(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalStr(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
