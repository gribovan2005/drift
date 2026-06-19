package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStageMetrics_Record_Counts(t *testing.T) {
	m := NewStageMetrics("test", nil)
	m.Record(10, time.Millisecond, nil)
	m.Record(5, time.Millisecond, nil)
	m.Record(0, 0, assert.AnError)

	snap := m.Snapshot()
	assert.Equal(t, int64(15), snap.ProcessedTotal)
	assert.Equal(t, int64(1), snap.ErrorTotal)
}

func TestStageMetrics_Throughput_ResetsOnSnapshot(t *testing.T) {
	m := NewStageMetrics("tp", nil)
	m.Record(100, time.Millisecond, nil)

	s1 := m.Snapshot()
	assert.Greater(t, s1.Throughput, 0.0)

	// Second snapshot with no new records → throughput ≈ 0 (or very low).
	s2 := m.Snapshot()
	assert.Less(t, s2.Throughput, s1.Throughput)
}

func TestStageMetrics_LatencyPercentiles(t *testing.T) {
	m := NewStageMetrics("lat", nil)

	// Feed 100 samples: 0ms, 1ms, …, 99ms.
	for i := 0; i < 100; i++ {
		m.Record(1, time.Duration(i)*time.Millisecond, nil)
	}
	snap := m.Snapshot()

	// p50 should be ~49ms, p99 should be ~98ms (floor index arithmetic).
	assert.InDelta(t, 49*time.Millisecond, snap.LatencyP50, float64(2*time.Millisecond))
	assert.InDelta(t, 98*time.Millisecond, snap.LatencyP99, float64(2*time.Millisecond))
}

func TestStageMetrics_QueueDepth(t *testing.T) {
	depth := int64(7)
	m := NewStageMetrics("q", func() int64 { return depth })

	snap := m.Snapshot()
	assert.Equal(t, int64(7), snap.QueueDepth)
}

func TestStageMetrics_EmptySnapshot(t *testing.T) {
	m := NewStageMetrics("empty", nil)
	snap := m.Snapshot()
	assert.Equal(t, "empty", snap.Label)
	assert.Zero(t, snap.ProcessedTotal)
	assert.Zero(t, snap.LatencyP50)
}
