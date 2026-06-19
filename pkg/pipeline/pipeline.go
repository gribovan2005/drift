package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/andrejgribov/drift/pkg/checkpoint"
	"github.com/andrejgribov/drift/pkg/core"
	"github.com/andrejgribov/drift/pkg/metrics"
)

const defaultBatchSize = 64
const defaultChannelBuf = 256

// Stage wraps an Operator with its label and channel buffer size.
type Stage struct {
	Label   string
	Op      core.Operator
	BufSize int // 0 → defaultChannelBuf
}

// GraphNode describes one stage in the pipeline DAG.
type GraphNode struct {
	Label string
	Next  []string // labels of direct downstream stages; empty for the last stage
}

// Option configures a Pipeline.
type Option func(*Pipeline)

// WithLogger sets the structured logger used by the pipeline.
// Defaults to slog.Default() if not provided.
func WithLogger(l *slog.Logger) Option {
	return func(p *Pipeline) { p.logger = l }
}

// WithCheckpoint enables operator state persistence. On startup the pipeline
// restores any saved state; on clean shutdown it saves current state.
func WithCheckpoint(store checkpoint.Store) Option {
	return func(p *Pipeline) { p.checkpoint = store }
}

// Pipeline connects a Source through a chain of Stages to a Sink.
// Each stage runs in its own goroutine and communicates via buffered channels.
// Metrics are collected automatically for every stage.
type Pipeline struct {
	source     core.Source
	stages     []Stage
	sink       core.Sink
	batchSize  int
	stageM     []*metrics.StageMetrics
	logger     *slog.Logger
	checkpoint checkpoint.Store
}

// New creates a Pipeline. Stages are executed in order; source feeds the
// first stage and the last stage feeds the sink.
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

// Graph returns a linear DAG description of the pipeline.
func (p *Pipeline) Graph() []GraphNode {
	nodes := make([]GraphNode, len(p.stages))
	for i, s := range p.stages {
		nodes[i] = GraphNode{Label: s.Label}
		if i+1 < len(p.stages) {
			nodes[i].Next = []string{p.stages[i+1].Label}
		}
	}
	return nodes
}

// IsReady reports whether any stage has processed at least one record.
// Used by the /readyz health endpoint.
func (p *Pipeline) IsReady() bool {
	for _, m := range p.stageM {
		if m.Snapshot().ProcessedTotal > 0 {
			return true
		}
	}
	return false
}

// Run starts the pipeline and blocks until completion or ctx cancellation.
// If any stage returns an error the pipeline is cancelled and the error is
// returned.
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

	ch := srcCh
	errChs := make([]<-chan error, len(p.stages))

	for i, stage := range p.stages {
		buf := stage.BufSize
		if buf == 0 {
			buf = defaultChannelBuf
		}
		inCh := ch
		outCh := make(chan core.Record, buf)
		errCh := make(chan error, 1)
		errChs[i] = errCh

		inChCopy := inCh
		p.stageM[i].SetQueueLen(func() int64 { return int64(len(inChCopy)) })

		stageLog := log.With("stage", stage.Label)
		stageLog.Info("stage starting")
		go runStage(ctx, cancel, stage.Op, inCh, outCh, errCh, p.batchSize, p.stageM[i], stageLog)
		ch = outCh
	}

	sinkErrCh := make(chan error, 1)
	go func() { sinkErrCh <- p.sink.Write(ctx, ch) }()

	var errs []error
	for i, errCh := range errChs {
		if e := <-errCh; e != nil {
			log.Error("stage error", "stage", p.stages[i].Label, "err", e)
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
// Called after all stage goroutines have exited — no concurrent Process calls.
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
	sm *metrics.StageMetrics,
	log *slog.Logger,
) {
	defer close(out)

	batch := make([]core.Record, 0, batchSize)

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
		case <-ctx.Done():
			errCh <- nil
			return
		}
	}
}
