package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/andrejgribov/drift/pkg/ai"
	"github.com/andrejgribov/drift/pkg/core"
	"github.com/andrejgribov/drift/pkg/dlq"
	"github.com/andrejgribov/drift/pkg/pipeline"
	"github.com/andrejgribov/drift/pkg/schema"
	"github.com/andrejgribov/drift/pkg/sink"
	"github.com/andrejgribov/drift/pkg/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testServer(t *testing.T, opts ...ServerOption) *Server {
	t.Helper()
	reg := schema.NewRegistry()
	p := pipeline.New(
		source.NewMemory(nil),
		[]pipeline.Stage{{Label: "noop", Op: noopOp{}}},
		sink.NewMemory(),
	)
	dbg := ai.New("", "")
	return New(":0", p, reg, dbg, opts...)
}

type noopOp struct{}

func (noopOp) Process(in []core.Record) ([]core.Record, error) { return in, nil }
func (noopOp) OnSchemaChange(_ core.Schema)                     {}

func TestHandleHealthz_AlwaysOK(t *testing.T) {
	s := testServer(t)
	rec := httptest.NewRecorder()
	s.handleHealthz(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	assert.Equal(t, http.StatusOK, rec.Code)
	var body map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	assert.Equal(t, "ok", body["status"])
}

func TestHandleReadyz_NotReadyBeforeRecords(t *testing.T) {
	s := testServer(t)
	rec := httptest.NewRecorder()
	s.handleReadyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestHandleReadyz_ReadyAfterPipelineRun(t *testing.T) {
	reg := schema.NewRegistry()
	records := []core.Record{
		{SchemaID: "x", Payload: map[string]any{"v": 1}},
	}
	p := pipeline.New(
		source.NewMemory(records),
		[]pipeline.Stage{{Label: "noop", Op: noopOp{}}},
		sink.NewMemory(),
	)
	// Run the pipeline so metrics are recorded.
	require.NoError(t, p.Run(context.Background()))

	s := New(":0", p, reg, ai.New("", ""))
	rec := httptest.NewRecorder()
	s.handleReadyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandleDLQ_NotConfigured(t *testing.T) {
	s := testServer(t)
	rec := httptest.NewRecorder()
	s.handleDLQ(rec, httptest.NewRequest(http.MethodGet, "/api/dlq", nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleDLQ_WithQueue(t *testing.T) {
	q := dlq.New()
	q.Add([]byte(`bad`), "unmarshal error", "payments")

	s := testServer(t, WithDLQ(q))
	rec := httptest.NewRecorder()
	s.handleDLQ(rec, httptest.NewRequest(http.MethodGet, "/api/dlq", nil))

	assert.Equal(t, http.StatusOK, rec.Code)
	var body map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	assert.Equal(t, float64(1), body["total"])
}
