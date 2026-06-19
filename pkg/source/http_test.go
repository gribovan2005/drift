package source

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/andrejgribov/drift/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPSource_IngestsRecords(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	src := NewHTTP(":0") // random port
	ch, err := src.Read(ctx)
	require.NoError(t, err)

	records := []core.Record{
		{Payload: map[string]any{"id": "a", "v": float64(1)}},
		{Payload: map[string]any{"id": "b", "v": float64(2)}},
	}
	body, _ := json.Marshal(records)

	resp, err := http.Post("http://"+src.Addr+"/ingest", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var got []core.Record
	deadline := time.After(2 * time.Second)
	for len(got) < len(records) {
		select {
		case r, ok := <-ch:
			if !ok {
				t.Fatal("channel closed before all records received")
			}
			got = append(got, r)
		case <-deadline:
			t.Fatalf("timeout: got %d/%d records", len(got), len(records))
		}
	}
	assert.Len(t, got, 2)
}

func TestHTTPSource_RejectsNonPost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	src := NewHTTP(":0")
	_, err := src.Read(ctx)
	require.NoError(t, err)

	resp, err := http.Get("http://" + src.Addr + "/ingest")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

func TestHTTPSource_RejectsBadJSON(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	src := NewHTTP(":0")
	_, err := src.Read(ctx)
	require.NoError(t, err)

	resp, err := http.Post("http://"+src.Addr+"/ingest", "application/json",
		bytes.NewReader([]byte("not-json")))
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
