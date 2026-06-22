package sink

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPSink_PostsRecords(t *testing.T) {
	var received []core.Record

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("content-type"))
		body, _ := io.ReadAll(r.Body)
		var rec core.Record
		require.NoError(t, json.Unmarshal(body, &rec))
		received = append(received, rec)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	snk := NewHTTP(srv.URL)
	ch := make(chan core.Record, 3)
	ch <- core.Record{Payload: map[string]any{"x": 1}}
	ch <- core.Record{Payload: map[string]any{"x": 2}}
	ch <- core.Record{Payload: map[string]any{"x": 3}}
	close(ch)

	require.NoError(t, snk.Write(context.Background(), ch))
	assert.Len(t, received, 3)
}

func TestHTTPSink_ErrorOnUpstreamFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "gone", http.StatusGone)
	}))
	defer srv.Close()

	snk := NewHTTP(srv.URL)
	ch := make(chan core.Record, 1)
	ch <- core.Record{Payload: map[string]any{"x": 1}}
	close(ch)

	err := snk.Write(context.Background(), ch)
	assert.ErrorContains(t, err, "410")
}
