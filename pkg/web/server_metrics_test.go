package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gribovan2005/drift/pkg/ai"
	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/pipeline"
	"github.com/gribovan2005/drift/pkg/schema"
	"github.com/gribovan2005/drift/pkg/sink"
	"github.com/gribovan2005/drift/pkg/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleMetrics_ServesPrometheus(t *testing.T) {
	records := []core.Record{{Payload: map[string]any{"v": 1}}}
	p := pipeline.New(
		source.NewMemory(records),
		[]pipeline.Stage{{Label: "noop", Op: noopOp{}}},
		sink.NewMemory(),
	)
	require.NoError(t, p.Run(context.Background()))

	s := New(":0", p, schema.NewRegistry(), ai.New("", ""))
	rec := httptest.NewRecorder()
	s.handleMetrics(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Header().Get("Content-Type"), "text/plain")
	body := rec.Body.String()
	assert.Contains(t, body, "drift_stage_processed_total")
	assert.Contains(t, body, `stage="noop"`)
}

func TestHandleMetrics_IdleEmpty(t *testing.T) {
	// staticProvider over a nil pipeline ⇒ Current("") returns nil ⇒ empty 200.
	s := New(":0", nil, schema.NewRegistry(), ai.New("", ""))
	rec := httptest.NewRecorder()
	s.handleMetrics(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.False(t, strings.Contains(rec.Body.String(), "{stage="), "idle scrape should have no series")
}
