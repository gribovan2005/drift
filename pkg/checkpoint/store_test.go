package checkpoint

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileStore_SaveLoad_Roundtrip(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStore(dir)
	require.NoError(t, err)

	data := []byte(`{"buf":[]}`)
	require.NoError(t, s.Save("tumbling-window", data))

	got, found, err := s.Load("tumbling-window")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, data, got)
}

func TestFileStore_Load_NotExists(t *testing.T) {
	s, err := NewFileStore(t.TempDir())
	require.NoError(t, err)

	_, found, err := s.Load("nonexistent")
	require.NoError(t, err)
	assert.False(t, found)
}

func TestFileStore_Save_Atomic(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStore(dir)
	require.NoError(t, err)

	require.NoError(t, s.Save("op", []byte("v1")))
	require.NoError(t, s.Save("op", []byte("v2")))

	// No .tmp file should remain after a successful save.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		assert.NotEqual(t, ".tmp", filepath.Ext(e.Name()))
	}

	got, _, _ := s.Load("op")
	assert.Equal(t, []byte("v2"), got)
}

func TestFileStore_Sanitize(t *testing.T) {
	cases := []struct{ in, want string }{
		{"simple", "simple"},
		{"with spaces", "with_spaces"},
		{"stage/nested", "stage_nested"},
		{"ok-dash", "ok-dash"},
		{"MixedCase123", "MixedCase123"},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, sanitize(c.in))
	}
}

func TestNewFileStore_CreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "deep", "nested")
	_, err := NewFileStore(dir)
	require.NoError(t, err)
	_, err = os.Stat(dir)
	assert.NoError(t, err)
}
