package runner

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/job"
	"github.com/gribovan2005/drift/pkg/pipeline"
)

// tapSize is how many recent records per stage the runner retains for the UI.
const tapSize = 25

// stopTimeout bounds how long Stop waits for a pipeline's Run to return.
const stopTimeout = 5 * time.Second

// JobStatus describes one running job.
type JobStatus struct {
	Name      string        `json:"name"`
	StartedAt time.Time     `json:"started_at"`
	Uptime    time.Duration `json:"uptime"`
}

// Status is the runner's overall state: every running job plus recent errors.
type Status struct {
	Running []JobStatus       `json:"running"`
	Errors  map[string]string `json:"errors,omitempty"`
}

// Option configures a Runner.
type Option func(*Runner)

// WithLogger sets the runner's logger.
func WithLogger(l *slog.Logger) Option { return func(r *Runner) { r.logger = l } }

// run holds one live pipeline.
type run struct {
	name      string
	pipeline  *pipeline.Pipeline
	tap       *pipeline.Tap
	cancel    context.CancelFunc
	done      chan struct{}
	startedAt time.Time
}

// Runner manages any number of concurrently running pipelines, keyed by job name.
// Each run is an independent pipeline (fresh operator state, built per Start via
// job.Load). All state is guarded by mu.
type Runner struct {
	store  *Store
	logger *slog.Logger

	mu   sync.RWMutex
	runs map[string]*run
	errs map[string]string // name → last error from a finished run
}

// New creates a Runner with no running jobs.
func New(store *Store, opts ...Option) *Runner {
	r := &Runner{
		store:  store,
		logger: slog.Default(),
		runs:   make(map[string]*run),
		errs:   make(map[string]string),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Store returns the underlying job store.
func (r *Runner) Store() *Store { return r.store }

// Start builds a fresh pipeline from the named job and runs it concurrently with
// any others. It errors only if that same job is already running.
func (r *Runner) Start(name string) error {
	r.mu.RLock()
	_, running := r.runs[name]
	r.mu.RUnlock()
	if running {
		return fmt.Errorf("runner: job %q is already running; stop it first", name)
	}

	path, err := r.store.path(name)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("runner: read %q: %w", name, err)
	}
	built, err := job.Load(data)
	if err != nil {
		return err
	}
	tap := pipeline.NewTap(tapSize)
	p := built.Pipeline(pipeline.WithLogger(r.logger), pipeline.WithTap(tap))

	ctx, cancel := context.WithCancel(context.Background())
	rn := &run{name: name, pipeline: p, tap: tap, cancel: cancel, done: make(chan struct{}), startedAt: time.Now()}

	r.mu.Lock()
	if _, dup := r.runs[name]; dup { // lost a race
		r.mu.Unlock()
		cancel()
		return fmt.Errorf("runner: job %q is already running; stop it first", name)
	}
	r.runs[name] = rn
	delete(r.errs, name)
	r.mu.Unlock()

	go func() {
		runErr := p.Run(ctx)
		r.finish(rn, runErr)
		close(rn.done)
	}()

	r.logger.Info("runner started job", "job", name)
	return nil
}

// finish removes a run when its pipeline exits on its own.
func (r *Runner) finish(rn *run, runErr error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.runs[rn.name] != rn {
		return // already superseded by Stop
	}
	delete(r.runs, rn.name)
	if runErr != nil && runErr != context.Canceled {
		r.errs[rn.name] = runErr.Error()
	}
}

// Stop cancels a running job and waits (bounded) for it to exit. Stopping a job
// that isn't running is a no-op.
func (r *Runner) Stop(name string) error {
	r.mu.RLock()
	rn := r.runs[name]
	r.mu.RUnlock()
	if rn == nil {
		return nil
	}

	rn.cancel()
	select {
	case <-rn.done:
	case <-time.After(stopTimeout):
		r.logger.Warn("runner: stop timed out; detaching", "job", name)
	}

	r.mu.Lock()
	if r.runs[name] == rn {
		delete(r.runs, name)
	}
	r.mu.Unlock()
	r.logger.Info("runner stopped job", "job", name)
	return nil
}

// StopAll stops every running job (used on shutdown).
func (r *Runner) StopAll() {
	r.mu.RLock()
	names := make([]string, 0, len(r.runs))
	for n := range r.runs {
		names = append(names, n)
	}
	r.mu.RUnlock()
	for _, n := range names {
		_ = r.Stop(n)
	}
}

// Current returns the pipeline for the named job, or nil if it isn't running.
// When name is empty and exactly one job is running, that one is returned (a
// convenience for single-pipeline setups).
func (r *Runner) Current(name string) *pipeline.Pipeline {
	rn := r.pick(name)
	if rn == nil {
		return nil
	}
	return rn.pipeline
}

// Sample returns recent records emitted by a stage of the named running job.
func (r *Runner) Sample(name, stage string) []core.Record {
	rn := r.pick(name)
	if rn == nil {
		return nil
	}
	return rn.tap.Sample(stage)
}

// pick resolves a run by name (or the sole run when name is empty).
func (r *Runner) pick(name string) *run {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if name == "" {
		if len(r.runs) == 1 {
			for _, rn := range r.runs {
				return rn
			}
		}
		return nil
	}
	return r.runs[name]
}

// Status returns every running job (sorted by name) plus recent errors.
func (r *Runner) Status() Status {
	r.mu.RLock()
	defer r.mu.RUnlock()
	st := Status{}
	for name, rn := range r.runs {
		st.Running = append(st.Running, JobStatus{Name: name, StartedAt: rn.startedAt, Uptime: time.Since(rn.startedAt)})
	}
	sort.Slice(st.Running, func(i, j int) bool { return st.Running[i].Name < st.Running[j].Name })
	if len(r.errs) > 0 {
		st.Errors = make(map[string]string, len(r.errs))
		for k, v := range r.errs {
			st.Errors[k] = v
		}
	}
	return st
}
