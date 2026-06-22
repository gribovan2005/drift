package wal_test

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/gribovan2005/drift/pkg/wal"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLog_AppendAssignsMonotonicLSN(t *testing.T) {
	l, err := wal.Open(t.TempDir())
	require.NoError(t, err)
	defer l.Close()

	for i := uint64(1); i <= 5; i++ {
		lsn, err := l.Append([]byte("rec"))
		require.NoError(t, err)
		assert.Equal(t, i, lsn)
	}
}

func TestLog_RecoverNextLSN(t *testing.T) {
	dir := t.TempDir()

	l, err := wal.Open(dir)
	require.NoError(t, err)
	for range 3 {
		_, err := l.Append([]byte("x"))
		require.NoError(t, err)
	}
	require.NoError(t, l.Close())

	// Reopen: numbering must continue past the max LSN.
	l2, err := wal.Open(dir)
	require.NoError(t, err)
	defer l2.Close()
	lsn, err := l2.Append([]byte("y"))
	require.NoError(t, err)
	assert.Equal(t, uint64(4), lsn)
}

func TestLog_CommitWatermarkPersists(t *testing.T) {
	dir := t.TempDir()

	l, err := wal.Open(dir)
	require.NoError(t, err)
	for range 5 {
		_, err := l.Append([]byte("x"))
		require.NoError(t, err)
	}
	require.NoError(t, l.Commit(3))
	require.NoError(t, l.Close())

	l2, err := wal.Open(dir)
	require.NoError(t, err)
	defer l2.Close()
	assert.Equal(t, uint64(3), l2.Committed())
}

func TestLog_UncommittedAfterCommit(t *testing.T) {
	l, err := wal.Open(t.TempDir())
	require.NoError(t, err)
	defer l.Close()

	for i := range 5 {
		_, err := l.Append([]byte{byte('a' + i)})
		require.NoError(t, err)
	}
	require.NoError(t, l.Commit(2))

	un, err := l.Uncommitted()
	require.NoError(t, err)
	require.Len(t, un, 3)
	assert.Equal(t, uint64(3), un[0].LSN)
	assert.Equal(t, []byte("c"), un[0].Data)
	assert.Equal(t, uint64(5), un[2].LSN)
}

func TestLog_TornFrameIgnored(t *testing.T) {
	dir := t.TempDir()

	l, err := wal.Open(dir)
	require.NoError(t, err)
	for range 2 {
		_, err := l.Append([]byte("good"))
		require.NoError(t, err)
	}
	require.NoError(t, l.Close())

	// Simulate a crash mid-append: a header claiming 100 bytes with no payload.
	f, err := os.OpenFile(filepath.Join(dir, "log.wal"), os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	hdr := make([]byte, 12)
	binary.LittleEndian.PutUint32(hdr[:4], 100)
	binary.LittleEndian.PutUint64(hdr[4:], 3)
	_, err = f.Write(hdr)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Recovery ignores the torn frame: next LSN is 3, not 4.
	l2, err := wal.Open(dir)
	require.NoError(t, err)
	defer l2.Close()
	un, err := l2.Uncommitted()
	require.NoError(t, err)
	require.Len(t, un, 2)
	lsn, err := l2.Append([]byte("next"))
	require.NoError(t, err)
	assert.Equal(t, uint64(3), lsn)
}

func TestLog_Concurrent(t *testing.T) {
	l, err := wal.Open(t.TempDir())
	require.NoError(t, err)
	defer l.Close()

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				_, err := l.Append([]byte("r"))
				assert.NoError(t, err)
				_ = l.Commit(1)
			}
		}()
	}
	wg.Wait()

	un, err := l.Uncommitted()
	require.NoError(t, err)
	// 200 appends total, 1 committed → 199 uncommitted.
	assert.Len(t, un, 199)
}
