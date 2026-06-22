package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gribovan2005/drift/pkg/checkpoint"
	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/lineage"
	"github.com/gribovan2005/drift/pkg/metrics"
)

const defaultBatchSize = 64
const defaultChannelBuf = 256

// Stage wraps an Operator with its label and channel buffer size.
// Next lists downstream stage labels for DAG wiring. When empty,
// the pipeline fills it in linearly (next stage in slice order, or sink).
type Stage struct {
	Label   string
	Op      core.Operator
	BufSize int      // 0 → defaultChannelBuf
	Next    []string // downstream stage labels; nil = linear order
}

// GraphNode describes one node in the pipeline DAG.
type GraphNode struct {
	Label string
	Next  []string // labels of direct downstream stages; empty for the last stage
}

// Option configures a Pipeline.
type Option func(*Pipeline)

// WithLogger sets the structured logger used by the pipeline.
func WithLogger(l *slog.Logger) Option {
	return func(p *Pipeline) { p.logger = l }
}

// WithCheckpoint enables operator state persistence.
func WithCheckpoint(store checkpoint.Store) Option {
	return func(p *Pipeline) { p.checkpoint = store }
}

// WithLineage enables record-level provenance tracking. Each stage operator is
// wrapped so that records are assigned IDs and their parentage is recorded in t.
// The wrapping preserves each operator's Flusher/Snapshottable behaviour, so
// windowing and checkpointing are unaffected. See pkg/lineage.
func WithLineage(t *lineage.Tracker) Option {
	return func(p *Pipeline) {
		for i := range p.stages {
			p.stages[i].Op = t.Wrap(p.stages[i].Label, p.stages[i].Op)
		}
	}
}

// WithBatchSize sets the per-stage batch size (records processed per Operator
// call). Larger favours throughput, smaller favours latency. Non-positive n is
// ignored. Default 64.
func WithBatchSize(n int) Option {
	return func(p *Pipeline) {
		if n > 0 {
			p.batchSize = n
		}
	}
}

// WithChannelBuffer sets the default inter-stage channel buffer. A per-stage
// Stage.BufSize still overrides this. Larger favours throughput (absorbs bursts),
// smaller favours a tighter memory footprint. Non-positive n is ignored.
// Default 256.
func WithChannelBuffer(n int) Option {
	return func(p *Pipeline) {
		if n > 0 {
			p.chanBuf = n
		}
	}
}

// WithMaxLinger enables a time-based partial-batch flush: a stage with a partial
// batch flushes at least every d, instead of waiting to fill batchSize or for the
// input to close. This is what lets a small batch size deliver low latency under
// sparse input. d<=0 (the default) disables it entirely — no timer is created and
// the data path is unchanged.
func WithMaxLinger(d time.Duration) Option {
	return func(p *Pipeline) {
		if d > 0 {
			p.linger = d
		}
	}
}

// Pipeline connects a Source through a DAG of Stages to a Sink.
// Each stage runs in its own goroutine and communicates via buffered channels.
// Metrics are collected automatically for every stage.
type Pipeline struct {
	source     core.Source
	stages     []Stage
	sink       core.Sink
	batchSize  int
	chanBuf    int           // default inter-stage channel buffer; Stage.BufSize overrides
	linger     time.Duration // >0 enables a time-based partial-batch flush
	stageM     []*metrics.StageMetrics
	logger     *slog.Logger
	checkpoint checkpoint.Store
	tap        *Tap // optional; captures recent per-stage output records
}

// New creates a Pipeline. When Stage.Next is unset, stages run in slice order.
func New(source core.Source, stages []Stage, sink core.Sink, opts ...Option) *Pipeline {
	sm := make([]*metrics.StageMetrics, len(stages))
	for i, s := range stages {
		sm[i] = metrics.NewStageMetrics(s.Label, nil)
	}
	p := &Pipeline{
		source:    source,
		stages:    stages,
		sink:      sink,
		batchSize: defaultBatchSize,
		chanBuf:   defaultChannelBuf,
		stageM:    sm,
		logger:    slog.Default(),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Snapshot returns a point-in-time view of every stage's metrics.
func (p *Pipeline) Snapshot() metrics.MetricsSnapshot {
	snap := metrics.MetricsSnapshot{CollectedAt: time.Now()}
	for _, m := range p.stageM {
		snap.Stages = append(snap.Stages, m.Snapshot())
	}
	return snap
}

// Graph returns the pipeline DAG, reflecting actual Next wiring.
func (p *Pipeline) Graph() []GraphNode {
	stages := p.resolveNext()
	nodes := make([]GraphNode, len(stages))
	for i, s := range stages {
		nodes[i] = GraphNode{Label: s.Label, Next: s.Next}
	}
	return nodes
}

// IsReady reports whether any stage has processed at least one record.
func (p *Pipeline) IsReady() bool {
	for _, m := range p.stageM {
		if m.Snapshot().ProcessedTotal > 0 {
			return true
		}
	}
	return false
}

// Run starts the pipeline and blocks until completion or ctx cancellation.
func (p *Pipeline) Run(ctx context.Context) error {
	start := time.Now()
	log := p.logger.With("component", "pipeline")

	p.restoreCheckpoints(log)

	log.Info("pipeline starting", "stages", len(p.stages))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	srcCh, err := p.source.Read(ctx)
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}

	stages := p.resolveNext()
	pred := buildPredMap(stages)

	// ── Build per-edge channels ───────────────────────────────────────────
	// edgeKey{from, to}: one buffered channel per directed edge.
	// Special labels: "source" (upstream of root stages), "sink" (downstream
	// of terminal stages).
	type edgeKey struct{ from, to string }
	edges := map[edgeKey]chan core.Record{}

	bufFor := func(label string) int {
		for _, s := range stages {
			if s.Label == label && s.BufSize > 0 {
				return s.BufSize
			}
		}
		return p.chanBuf
	}

	for _, s := range stages {
		if len(pred[s.Label]) == 0 {
			edges[edgeKey{"source", s.Label}] = make(chan core.Record, bufFor(s.Label))
		}
		for _, next := range s.Next {
			edges[edgeKey{s.Label, next}] = make(chan core.Record, bufFor(next))
		}
		if len(s.Next) == 0 {
			edges[edgeKey{s.Label, "sink"}] = make(chan core.Record, p.chanBuf)
		}
	}

	// ── Wire source → root stages ─────────────────────────────────────────
	roots := rootLabels(stages, pred)
	rootDsts := make([]chan core.Record, len(roots))
	for i, r := range roots {
		rootDsts[i] = edges[edgeKey{"source", r}]
	}
	go broadcastAll(ctx, srcCh, rootDsts)

	// ── Per-stage output channel (stage goroutine writes here) ────────────
	stageOut := make(map[string]chan core.Record, len(stages))
	for _, s := range stages {
		stageOut[s.Label] = make(chan core.Record, bufFor(s.Label))
	}

	// ── Wire stage outputs → downstream edge channels ─────────────────────
	for _, s := range stages {
		var dsts []chan core.Record
		for _, next := range s.Next {
			dsts = append(dsts, edges[edgeKey{s.Label, next}])
		}
		if len(s.Next) == 0 {
			dsts = []chan core.Record{edges[edgeKey{s.Label, "sink"}]}
		}
		go broadcastAll(ctx, stageOut[s.Label], dsts)
	}

	// ── Build stage input channels (fan-in merger if multiple preds) ──────
	stageIn := make(map[string]<-chan core.Record, len(stages))
	for _, s := range stages {
		var srcs []chan core.Record
		if ch, ok := edges[edgeKey{"source", s.Label}]; ok {
			srcs = append(srcs, ch)
		}
		for _, pr := range pred[s.Label] {
			srcs = append(srcs, edges[edgeKey{pr, s.Label}])
		}
		if len(srcs) == 1 {
			stageIn[s.Label] = srcs[0]
		} else {
			merged := make(chan core.Record, bufFor(s.Label))
			go mergeAll(ctx, srcs, merged)
			stageIn[s.Label] = merged
		}
	}

	// ── Collect sink input channels ───────────────────────────────────────
	var sinkSrcs []chan core.Record
	for _, s := range stages {
		if len(s.Next) == 0 {
			sinkSrcs = append(sinkSrcs, edges[edgeKey{s.Label, "sink"}])
		}
	}
	var sinkIn <-chan core.Record
	if len(sinkSrcs) == 1 {
		sinkIn = sinkSrcs[0]
	} else {
		merged := make(chan core.Record, p.chanBuf)
		go mergeAll(ctx, sinkSrcs, merged)
		sinkIn = merged
	}

	// ── Start stage goroutines ────────────────────────────────────────────
	errChs := make([]<-chan error, len(stages))
	for i, stage := range stages {
		errCh := make(chan error, 1)
		errChs[i] = errCh
		in := stageIn[stage.Label]
		p.stageM[i].SetQueueLen(func() int64 { return int64(len(in)) })
		stageLog := log.With("stage", stage.Label)
		stageLog.Info("stage starting")
		go runStage(ctx, cancel, stage.Op, in, stageOut[stage.Label], errCh, p.batchSize, p.linger, p.stageM[i], stageLog, p.tap, stage.Label)
	}

	sinkErrCh := make(chan error, 1)
	go func() { sinkErrCh <- p.sink.Write(ctx, sinkIn) }()

	var errs []error
	for i, errCh := range errChs {
		if e := <-errCh; e != nil {
			log.Error("stage error", "stage", stages[i].Label, "err", e)
			errs = append(errs, e)
		}
	}
	if e := <-sinkErrCh; e != nil {
		errs = append(errs, e)
	}

	p.saveCheckpoints(log)

	log.Info("pipeline stopped", "elapsed", time.Since(start).Round(time.Millisecond))

	if len(errs) > 0 {
		return fmt.Errorf("pipeline errors: %v", errs)
	}
	return nil
}

// resolveNext returns a copy of p.stages with linear Next filled in for
// any stage that didn't set it explicitly.
func (p *Pipeline) resolveNext() []Stage {
	stages := make([]Stage, len(p.stages))
	copy(stages, p.stages)
	for i := range stages {
		if len(stages[i].Next) == 0 && i+1 < len(stages) {
			stages[i].Next = []string{stages[i+1].Label}
		}
	}
	return stages
}

// buildPredMap returns label → list of predecessor labels.
func buildPredMap(stages []Stage) map[string][]string {
	pred := map[string][]string{}
	for _, s := range stages {
		for _, next := range s.Next {
			pred[next] = append(pred[next], s.Label)
		}
	}
	return pred
}

// rootLabels returns labels of stages with no predecessors.
func rootLabels(stages []Stage, pred map[string][]string) []string {
	var roots []string
	for _, s := range stages {
		if len(pred[s.Label]) == 0 {
			roots = append(roots, s.Label)
		}
	}
	return roots
}

// broadcastAll reads from src and copies each record to every dst.
// Closes all dsts when src is exhausted or ctx is cancelled.
func broadcastAll(ctx context.Context, src <-chan core.Record, dsts []chan core.Record) {
	defer func() {
		for _, dst := range dsts {
			close(dst)
		}
	}()
	for {
		select {
		case r, ok := <-src:
			if !ok {
				return
			}
			for _, dst := range dsts {
				select {
				case dst <- r:
				case <-ctx.Done():
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// mergeAll reads concurrently from all srcs and writes to dst.
// Closes dst when all srcs are exhausted or ctx is cancelled.
func mergeAll(ctx context.Context, srcs []chan core.Record, dst chan core.Record) {
	var wg sync.WaitGroup
	for _, src := range srcs {
		wg.Add(1)
		go func(ch <-chan core.Record) {
			defer wg.Done()
			for {
				select {
				case r, ok := <-ch:
					if !ok {
						return
					}
					select {
					case dst <- r:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(src)
	}
	wg.Wait()
	close(dst)
}

// restoreCheckpoints loads saved state for any Snapshottable stage operators.
func (p *Pipeline) restoreCheckpoints(log *slog.Logger) {
	if p.checkpoint == nil {
		return
	}
	for _, stage := range p.stages {
		sn, ok := stage.Op.(core.Snapshottable)
		if !ok {
			continue
		}
		data, found, err := p.checkpoint.Load(stage.Label)
		if err != nil {
			log.Warn("checkpoint load failed", "stage", stage.Label, "err", err)
			continue
		}
		if !found {
			continue
		}
		if err := sn.Restore(data); err != nil {
			log.Warn("checkpoint restore failed", "stage", stage.Label, "err", err)
			continue
		}
		log.Info("checkpoint restored", "stage", stage.Label)
	}
}

// saveCheckpoints persists state for any Snapshottable stage operators.
func (p *Pipeline) saveCheckpoints(log *slog.Logger) {
	if p.checkpoint == nil {
		return
	}
	for _, stage := range p.stages {
		sn, ok := stage.Op.(core.Snapshottable)
		if !ok {
			continue
		}
		data, err := sn.Snapshot()
		if err != nil {
			log.Error("checkpoint snapshot failed", "stage", stage.Label, "err", err)
			continue
		}
		if err := p.checkpoint.Save(stage.Label, data); err != nil {
			log.Error("checkpoint save failed", "stage", stage.Label, "err", err)
			continue
		}
		log.Info("checkpoint saved", "stage", stage.Label)
	}
}

// runStage reads from in, batches records, calls op.Process, and writes
// results to out. Closes out when in is exhausted or ctx is cancelled.
func runStage(
	ctx context.Context,
	cancel context.CancelFunc,
	op core.Operator,
	in <-chan core.Record,
	out chan<- core.Record,
	errCh chan<- error,
	batchSize int,
	linger time.Duration,
	sm *metrics.StageMetrics,
	log *slog.Logger,
	tap *Tap,
	label string,
) {
	defer close(out)

	batch := make([]core.Record, 0, batchSize)

	// Optional time-based partial-batch flush. linger<=0 leaves tickC nil, so its
	// select case never fires — zero overhead, identical to the size-only path.
	var tickC <-chan time.Time
	if linger > 0 {
		t := time.NewTicker(linger)
		defer t.Stop()
		tickC = t.C
	}

	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		start := time.Now()
		result, err := op.Process(batch)
		sm.Record(len(batch), time.Since(start), err)
		batch = batch[:0]
		if err != nil {
			log.Error("process error", "err", err)
			cancel()
			errCh <- err
			return false
		}
		tap.record(label, result)
		for _, r := range result {
			select {
			case out <- r:
			case <-ctx.Done():
				errCh <- nil
				return false
			}
		}
		return true
	}

	for {
		select {
		case r, ok := <-in:
			if !ok {
				if !flush() {
					return
				}
				if f, ok := op.(core.Flusher); ok {
					result, err := f.Flush()
					if err != nil {
						log.Error("flush error", "err", err)
						cancel()
						errCh <- err
						return
					}
					tap.record(label, result)
					for _, r := range result {
						select {
						case out <- r:
						case <-ctx.Done():
							errCh <- nil
							return
						}
					}
				}
				log.Info("stage done")
				errCh <- nil
				return
			}
			batch = append(batch, r)
			if len(batch) >= batchSize {
				if !flush() {
					return
				}
			}
		case <-tickC:
			// Linger timeout: emit whatever has accumulated (no-op if empty).
			if !flush() {
				return
			}
		case <-ctx.Done():
			errCh <- nil
			return
		}
	}
}
