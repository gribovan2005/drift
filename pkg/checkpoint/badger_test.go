package checkpoint

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBadgerStore_RoundTrip(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.Save("op1", []byte("hello")))

	data, found, err := s.Load("op1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("hello"), data)
}

func TestBadgerStore_NotFound(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	defer s.Close()

	_, found, err := s.Load("nonexistent")
	require.NoError(t, err)
	assert.False(t, found)
}

func TestBadgerStore_Overwrite(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.Save("op1", []byte("v1")))
	require.NoError(t, s.Save("op1", []byte("v2")))

	data, found, err := s.Load("op1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("v2"), data)
}

func TestBadgerStore_MultipleKeys(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.Save("op1", []byte("state1")))
	require.NoError(t, s.Save("op2", []byte("state2")))

	d1, _, _ := s.Load("op1")
	d2, _, _ := s.Load("op2")
	assert.Equal(t, []byte("state1"), d1)
	assert.Equal(t, []byte("state2"), d2)
}

func TestBadgerStore_Persistence(t *testing.T) {
	dir := t.TempDir()

	s1, err := NewBadgerStore(dir)
	require.NoError(t, err)
	require.NoError(t, s1.Save("op1", []byte("persisted")))
	require.NoError(t, s1.Close())

	s2, err := NewBadgerStore(dir)
	require.NoError(t, err)
	defer s2.Close()

	data, found, err := s2.Load("op1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("persisted"), data)
}

func TestBadgerStore_ImplementsStore(t *testing.T) {
	// Compile-time check: BadgerStore satisfies the Store interface.
	var _ Store = (*BadgerStore)(nil)
}
