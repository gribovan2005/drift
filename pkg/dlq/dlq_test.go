package dlq

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueue_Add_Snapshot(t *testing.T) {
	q := New()
	q.Add([]byte(`bad json`), "unmarshal error", "payments")

	recs, total := q.Snapshot()
	require.Len(t, recs, 1)
	assert.Equal(t, int64(1), total)
	assert.Equal(t, "unmarshal error", recs[0].Reason)
	assert.Equal(t, "payments", recs[0].Topic)
	assert.Equal(t, []byte(`bad json`), recs[0].Raw)
	assert.False(t, recs[0].Timestamp.IsZero())
}

func TestQueue_CapAtMaxRecords(t *testing.T) {
	q := New()
	for i := range maxRecords + 10 {
		q.Add([]byte(fmt.Sprintf("msg-%d", i)), "err", "t")
	}
	recs, total := q.Snapshot()
	assert.Equal(t, int64(maxRecords+10), total)
	assert.Len(t, recs, maxRecords)
	// Oldest entries dropped — first buffered entry should be #10
	assert.Contains(t, string(recs[0].Raw), "msg-10")
}

func TestQueue_Total_MonotonicallyIncreases(t *testing.T) {
	q := New()
	for range 5 {
		q.Add([]byte("x"), "e", "")
	}
	assert.Equal(t, int64(5), q.Total())
}

func TestQueue_Concurrent(t *testing.T) {
	q := New()
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			q.Add([]byte("data"), "reason", "topic")
		}()
	}
	wg.Wait()
	assert.Equal(t, int64(100), q.Total())
}

func TestQueue_SnapshotIsCopy(t *testing.T) {
	q := New()
	q.Add([]byte("original"), "e", "")
	recs, _ := q.Snapshot()
	recs[0].Reason = "mutated"

	recs2, _ := q.Snapshot()
	assert.Equal(t, "e", recs2[0].Reason, "snapshot must be independent of caller mutations")
}
