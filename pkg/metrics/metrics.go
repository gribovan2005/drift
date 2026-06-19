package metrics

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const ringSize = 1000

// OperatorMetrics is a point-in-time snapshot of one stage's health.
type OperatorMetrics struct {
	Label          string
	QueueDepth     int64
	ProcessedTotal int64
	ErrorTotal     int64
	LatencyP50     time.Duration
	LatencyP99     time.Duration
	Throughput     float64 // records/sec over the last measurement window
}

// MetricsSnapshot groups per-stage metrics collected at a single instant.
type MetricsSnapshot struct {
	CollectedAt time.Time
	Stages      []OperatorMetrics
}

// StageMetrics tracks runtime metrics for a single pipeline stage.
// All methods are goroutine-safe.
type StageMetrics struct {
	Label string

	processedTotal atomic.Int64
	errorTotal     atomic.Int64

	// latency ring buffer — stores last ringSize per-batch durations
	latMu   sync.Mutex
	latBuf  []time.Duration
	latPos  int
	latFull bool

	// throughput window — reset on each Snapshot call
	winMu    sync.Mutex
	winStart time.Time
	winCount int64

	// queueLen is a zero-argument func injected by the pipeline so that
	// Snapshot can read the input channel length without importing core.
	queueLen func() int64
}

// NewStageMetrics creates a StageMetrics. queueLen may be nil if queue depth
// tracking is not needed.
func NewStageMetrics(label string, queueLen func() int64) *StageMetrics {
	return &StageMetrics{
		Label:    label,
		latBuf:   make([]time.Duration, ringSize),
		winStart: time.Now(),
		queueLen: queueLen,
	}
}

// SetQueueLen replaces the queue-depth probe. Called by the pipeline once
// the stage's input channel is created.
func (m *StageMetrics) SetQueueLen(fn func() int64) {
	m.queueLen = fn
}

// Record registers the outcome of one op.Process call that processed n records
// and took duration d.
func (m *StageMetrics) Record(n int, d time.Duration, err error) {
	if err != nil {
		m.errorTotal.Add(1)
		return
	}
	m.processedTotal.Add(int64(n))

	m.latMu.Lock()
	m.latBuf[m.latPos] = d
	m.latPos++
	if m.latPos >= ringSize {
		m.latPos = 0
		m.latFull = true
	}
	m.latMu.Unlock()

	m.winMu.Lock()
	m.winCount += int64(n)
	m.winMu.Unlock()
}

// Snapshot returns a point-in-time view and resets the throughput window.
func (m *StageMetrics) Snapshot() OperatorMetrics {
	// latency percentiles from ring buffer copy
	m.latMu.Lock()
	size := m.latPos
	if m.latFull {
		size = ringSize
	}
	latCopy := make([]time.Duration, size)
	copy(latCopy, m.latBuf[:size])
	m.latMu.Unlock()

	sort.Slice(latCopy, func(i, j int) bool { return latCopy[i] < latCopy[j] })
	var p50, p99 time.Duration
	if n := len(latCopy); n > 0 {
		p50 = latCopy[n*50/100]
		p99 = latCopy[n*99/100]
	}

	// throughput over current window
	m.winMu.Lock()
	elapsed := time.Since(m.winStart).Seconds()
	count := m.winCount
	m.winStart = time.Now()
	m.winCount = 0
	m.winMu.Unlock()

	var throughput float64
	if elapsed > 0 {
		throughput = float64(count) / elapsed
	}

	var qd int64
	if m.queueLen != nil {
		qd = m.queueLen()
	}

	return OperatorMetrics{
		Label:          m.Label,
		QueueDepth:     qd,
		ProcessedTotal: m.processedTotal.Load(),
		ErrorTotal:     m.errorTotal.Load(),
		LatencyP50:     p50,
		LatencyP99:     p99,
		Throughput:     throughput,
	}
}
