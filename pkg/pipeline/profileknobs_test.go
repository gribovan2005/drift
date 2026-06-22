package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/operator"
	"github.com/gribovan2005/drift/pkg/sink"
	"github.com/gribovan2005/drift/pkg/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func identityStage() Stage {
	return Stage{Label: "id", Op: operator.NewMap(func(r core.Record) (core.Record, error) { return r, nil })}
}

func TestWithBatchSize_LastWins(t *testing.T) {
	p := New(source.NewMemory(nil), []Stage{identityStage()}, sink.NewMemory(),
		WithBatchSize(100), WithBatchSize(7))
	assert.Equal(t, 7, p.batchSize)
}

func TestWithChannelBuffer_SetsField(t *testing.T) {
	p := New(source.NewMemory(nil), []Stage{identityStage()}, sink.NewMemory(),
		WithChannelBuffer(13))
	assert.Equal(t, 13, p.chanBuf)
	// Non-positive ignored → default retained.
	p2 := New(source.NewMemory(nil), []Stage{identityStage()}, sink.NewMemory(), WithChannelBuffer(0))
	assert.Equal(t, defaultChannelBuf, p2.chanBuf)
}

func TestTinyBuffer_NoDeadlock(t *testing.T) {
	records := makeRecords(200)
	snk := sink.NewMemory()
	p := New(source.NewMemory(records), []Stage{identityStage()}, snk,
		WithChannelBuffer(1), WithBatchSize(1))
	require.NoError(t, p.Run(context.Background()))
	assert.Len(t, snk.Records(), 200)
}

// blockingSource emits n records, then blocks (does not close) until ctx is done.
// This simulates a live, sparse stream that never reaches the default batch size.
type blockingSource struct{ n int }

func (b *blockingSource) Read(ctx context.Context) (<-chan core.Record, error) {
	ch := make(chan core.Record)
	go func() {
		defer close(ch)
		for i := 0; i < b.n; i++ {
			select {
			case ch <- core.Record{Payload: map[string]any{"v": i}}:
			case <-ctx.Done():
				return
			}
		}
		<-ctx.Done() // keep the stream open but idle
	}()
	return ch, nil
}

func waitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

func TestMaxLinger_FlushesPartialBatch(t *testing.T) {
	// 3 records, default batch 64, source never closes → only a linger flush can
	// move them to the sink.
	snk := sink.NewMemory()
	p := New(&blockingSource{n: 3}, []Stage{identityStage()}, snk,
		WithMaxLinger(20*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	if !waitFor(func() bool { return len(snk.Records()) == 3 }, 2*time.Second) {
		t.Fatalf("linger did not flush partial batch; got %d records", len(snk.Records()))
	}
	cancel()
	<-done
}

func TestNoLinger_PartialBatchStalls(t *testing.T) {
	// Same setup without linger: the partial batch must NOT reach the sink (it
	// waits for batch size or channel close, neither of which happens).
	snk := sink.NewMemory()
	p := New(&blockingSource{n: 3}, []Stage{identityStage()}, snk)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	time.Sleep(150 * time.Millisecond)
	got := len(snk.Records())
	cancel()
	<-done
	assert.Equal(t, 0, got, "partial batch should stall without linger")
}
