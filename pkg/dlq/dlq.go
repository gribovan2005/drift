// Package dlq provides a thread-safe dead-letter queue for records that
// cannot be decoded or processed. It is an in-memory ring buffer capped at
// maxRecords; oldest entries are dropped when the cap is exceeded.
package dlq

import (
	"sync"
	"time"
)

const maxRecords = 1000

// FailedRecord captures a message that could not be decoded.
type FailedRecord struct {
	Raw       []byte    `json:"raw"`
	Reason    string    `json:"reason"`
	Topic     string    `json:"topic,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// Queue is a bounded ring buffer for failed records.
type Queue struct {
	mu      sync.Mutex
	records []FailedRecord
	total   int64 // cumulative count, never decreases
}

// New creates an empty Queue.
func New() *Queue { return &Queue{} }

// Add appends a failed record. Thread-safe.
func (q *Queue) Add(raw []byte, reason, topic string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.total++
	cp := make([]byte, len(raw))
	copy(cp, raw)
	q.records = append(q.records, FailedRecord{
		Raw:       cp,
		Reason:    reason,
		Topic:     topic,
		Timestamp: time.Now(),
	})
	if len(q.records) > maxRecords {
		q.records = q.records[len(q.records)-maxRecords:]
	}
}

// Snapshot returns a copy of buffered records and the cumulative total.
func (q *Queue) Snapshot() ([]FailedRecord, int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]FailedRecord, len(q.records))
	copy(out, q.records)
	return out, q.total
}

// Total returns the cumulative number of failed records since creation.
func (q *Queue) Total() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.total
}
