package sdk_test

import (
	"context"
	"testing"
	"time"

	"github.com/gribovan2005/drift/sdk"
)

// blockingSrc emits n records then blocks (does not close) until ctx is done.
type blockingSrc struct{ n int }

func (b *blockingSrc) Read(ctx context.Context) (<-chan sdk.Record, error) {
	ch := make(chan sdk.Record)
	go func() {
		defer close(ch)
		for i := 0; i < b.n; i++ {
			select {
			case ch <- sdk.Record{Payload: map[string]any{"v": i}}:
			case <-ctx.Done():
				return
			}
		}
		<-ctx.Done()
	}()
	return ch, nil
}

func poll(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

func TestWithProfile_AppliesLocalKnobs(t *testing.T) {
	// Sidecar has MaxLinger=5ms and batch=16. A sparse source of 3 records that
	// never closes only reaches the sink if the linger flush (a local knob) is in
	// effect — proving WithProfile wired it.
	c := sdk.Collect()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- sdk.New(sdk.WithProfile(sdk.Sidecar)).
			From(&blockingSrc{n: 3}).
			To(c).
			Run(ctx)
	}()

	if !poll(func() bool { return len(c.Records()) == 3 }, 2*time.Second) {
		t.Fatalf("Sidecar linger did not flush; got %d", len(c.Records()))
	}
	cancel()
	<-done
}

func TestGranularOverride_WinsOverProfile(t *testing.T) {
	// Dedicated sets batch=512, linger=0. Overriding to batch=3 means a source of
	// exactly 3 records fills the batch and flushes even though the source never
	// closes and there is no linger. If the override did NOT win (batch 512), the
	// 3 records would stall forever.
	c := sdk.Collect()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- sdk.New(sdk.WithProfile(sdk.Dedicated), sdk.WithBatchSize(3)).
			From(&blockingSrc{n: 3}).
			To(c).
			Run(ctx)
	}()

	if !poll(func() bool { return len(c.Records()) == 3 }, 2*time.Second) {
		t.Fatalf("override batch=3 did not flush; got %d (profile batch should be overridden)", len(c.Records()))
	}
	cancel()
	<-done
}
