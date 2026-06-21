package operator

import (
	"testing"
	"time"

	"github.com/andrejgribov/drift/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rec(id string) core.Record {
	return core.Record{Payload: map[string]any{"id": id}}
}

func keyByID(r core.Record) string {
	if v, ok := r.Payload["id"].(string); ok {
		return v
	}
	return ""
}

func newDedupWithClock(keyFn KeyFunc, window time.Duration, nowFn func() time.Time) *Deduplicate {
	d := NewDeduplicate(keyFn, window)
	d.nowFn = nowFn
	return d
}

func TestDedup_HappyPath(t *testing.T) {
	d := NewDeduplicate(keyByID, time.Hour)
	in := []core.Record{rec("a"), rec("b"), rec("c")}
	out, err := d.Process(in)
	require.NoError(t, err)
	assert.Len(t, out, 3)
}

func TestDedup_DropsWithinWindow(t *testing.T) {
	d := NewDeduplicate(keyByID, time.Hour)
	in := []core.Record{rec("a"), rec("a")}
	out, err := d.Process(in)
	require.NoError(t, err)
	assert.Len(t, out, 1)
	assert.Equal(t, "a", out[0].Payload["id"])
}

func TestDedup_PassesAfterExpiry(t *testing.T) {
	now := time.Now()
	nowFn := func() time.Time { return now }
	d := newDedupWithClock(keyByID, time.Second, nowFn)

	out, err := d.Process([]core.Record{rec("a")})
	require.NoError(t, err)
	assert.Len(t, out, 1)

	// Advance clock past the window.
	now = now.Add(2 * time.Second)

	out, err = d.Process([]core.Record{rec("a")})
	require.NoError(t, err)
	assert.Len(t, out, 1)
}

func TestDedup_OnSchemaChange_Concurrent(t *testing.T) {
	d := NewDeduplicate(keyByID, time.Hour)
	s := core.Schema{ID: "s", Version: 1}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			d.OnSchemaChange(s)
		}
	}()
	for i := 0; i < 100; i++ {
		d.Process([]core.Record{rec("x")}) //nolint:errcheck
	}
	<-done
}

func TestDedup_EmptyInput(t *testing.T) {
	d := NewDeduplicate(keyByID, time.Hour)
	out, err := d.Process(nil)
	require.NoError(t, err)
	assert.Nil(t, out)
}

func TestDedup_WindowZero(t *testing.T) {
	d := NewDeduplicate(keyByID, 0)
	in := []core.Record{rec("a"), rec("a"), rec("a")}
	out, err := d.Process(in)
	require.NoError(t, err)
	assert.Len(t, out, 3)
}

func TestDedup_Snapshot_Restore(t *testing.T) {
	d := NewDeduplicate(keyByID, time.Hour)

	_, err := d.Process([]core.Record{rec("a")})
	require.NoError(t, err)

	snap, err := d.Snapshot()
	require.NoError(t, err)

	d2 := NewDeduplicate(keyByID, time.Hour)
	require.NoError(t, d2.Restore(snap))

	out, err := d2.Process([]core.Record{rec("a")})
	require.NoError(t, err)
	assert.Len(t, out, 0, "duplicate must still be blocked after restore")
}

func TestDedup_Snapshot_RestoreDropsExpired(t *testing.T) {
	now := time.Now()
	nowFn := func() time.Time { return now }
	d := newDedupWithClock(keyByID, time.Second, nowFn)

	_, err := d.Process([]core.Record{rec("a")})
	require.NoError(t, err)

	snap, err := d.Snapshot()
	require.NoError(t, err)

	// Advance clock past window before restoring.
	now = now.Add(2 * time.Second)

	d2 := newDedupWithClock(keyByID, time.Second, nowFn)
	require.NoError(t, d2.Restore(snap))

	out, err := d2.Process([]core.Record{rec("a")})
	require.NoError(t, err)
	assert.Len(t, out, 1, "expired key must pass after restore")
}

func TestDedup_Snapshot_InvalidData(t *testing.T) {
	d := NewDeduplicate(keyByID, time.Hour)
	err := d.Restore([]byte("not json"))
	assert.Error(t, err)
}
