package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/andrejgribov/drift/pkg/core"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileSink_WritesNDJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.ndjson")
	s := NewFile(path)

	ch := make(chan core.Record, 3)
	ch <- core.Record{Payload: map[string]any{"v": 1}}
	ch <- core.Record{Payload: map[string]any{"v": 2}}
	ch <- core.Record{Payload: map[string]any{"v": 3}}
	close(ch)
	require.NoError(t, s.Write(context.Background(), ch))

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	var lines int
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var rec core.Record
		require.NoError(t, json.Unmarshal(sc.Bytes(), &rec))
		lines++
	}
	assert.Equal(t, 3, lines)
}
