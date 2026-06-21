package web

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/andrejgribov/drift/pkg/ai"
	"github.com/andrejgribov/drift/pkg/core"
	"github.com/andrejgribov/drift/pkg/job"
	"github.com/andrejgribov/drift/pkg/runner"
	"github.com/andrejgribov/drift/pkg/schema"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cpJob(name string) job.Spec {
	return job.Spec{
		Name:   name,
		Source: job.ComponentSpec{Type: "generator", Params: map[string]any{"rate": "1ms", "fields": map[string]any{"id": "seq"}}},
		Stages: []job.StageSpec{{Label: "tag", Op: "map-set", Params: map[string]any{"field": "x", "value": 1}}},
		Sink:   job.ComponentSpec{Type: "memory"},
	}
}

// cpServer builds a control-plane server backed by a temp-dir store.
func cpServer(t *testing.T, opts ...ServerOption) (*httptest.Server, *runner.Runner) {
	t.Helper()
	st, err := runner.NewStore(t.TempDir())
	require.NoError(t, err)
	r := runner.New(st)
	reg := schema.NewRegistry()
	base := []ServerOption{WithRunner(r)}
	s := New(":0", nil, reg, ai.New("", ""), append(base, opts...)...)
	h, err := s.routes()
	require.NoError(t, err)
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	t.Cleanup(func() { r.StopAll() })
	return ts, r
}

func doJSON(t *testing.T, method, url string, body any) *http.Response {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, url, rdr)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func TestServer_Palette(t *testing.T) {
	ts, _ := cpServer(t)
	resp := doJSON(t, http.MethodGet, ts.URL+"/api/palette", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var pal job.Palette
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&pal))
	assert.NotEmpty(t, pal.Sources)
	assert.NotEmpty(t, pal.Operators)
	assert.NotEmpty(t, pal.Sinks)
}

func TestServer_JobsCRUD(t *testing.T) {
	ts, _ := cpServer(t)

	// Create
	resp := doJSON(t, http.MethodPost, ts.URL+"/api/jobs", cpJob("alpha"))
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// List
	resp = doJSON(t, http.MethodGet, ts.URL+"/api/jobs", nil)
	var list []runner.JobInfo
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&list))
	resp.Body.Close()
	require.Len(t, list, 1)
	assert.Equal(t, "alpha", list[0].Name)

	// Get
	resp = doJSON(t, http.MethodGet, ts.URL+"/api/jobs/alpha", nil)
	var spec job.Spec
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&spec))
	resp.Body.Close()
	assert.Equal(t, "alpha", spec.Name)

	// Delete
	resp = doJSON(t, http.MethodDelete, ts.URL+"/api/jobs/alpha", nil)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = doJSON(t, http.MethodGet, ts.URL+"/api/jobs", nil)
	list = nil
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&list))
	resp.Body.Close()
	assert.Empty(t, list)
}

func TestServer_Validate(t *testing.T) {
	ts, _ := cpServer(t)

	resp := doJSON(t, http.MethodPost, ts.URL+"/api/validate", cpJob("ok"))
	var good map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&good))
	resp.Body.Close()
	assert.Equal(t, true, good["ok"])

	bad := cpJob("bad")
	bad.Stages[0].Op = "nope"
	resp = doJSON(t, http.MethodPost, ts.URL+"/api/validate", bad)
	var res map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&res))
	resp.Body.Close()
	assert.Equal(t, false, res["ok"])
	assert.NotEmpty(t, res["error"])
}

func TestServer_RunStopStatus(t *testing.T) {
	ts, r := cpServer(t)

	resp := doJSON(t, http.MethodPost, ts.URL+"/api/jobs", cpJob("alpha"))
	resp.Body.Close()

	resp = doJSON(t, http.MethodPost, ts.URL+"/api/run", map[string]string{"name": "alpha"})
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, r.Status().Running, 1)

	resp = doJSON(t, http.MethodGet, ts.URL+"/api/status", nil)
	var st runner.Status
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&st))
	resp.Body.Close()
	require.Len(t, st.Running, 1)
	assert.Equal(t, "alpha", st.Running[0].Name)

	resp = doJSON(t, http.MethodPost, ts.URL+"/api/stop", map[string]string{"name": "alpha"})
	resp.Body.Close()
	assert.Empty(t, r.Status().Running)
}

func TestServer_MultipleJobsConcurrent(t *testing.T) {
	ts, r := cpServer(t)
	for _, n := range []string{"alpha", "beta"} {
		doJSON(t, http.MethodPost, ts.URL+"/api/jobs", cpJob(n)).Body.Close()
		doJSON(t, http.MethodPost, ts.URL+"/api/run", map[string]string{"name": n}).Body.Close()
	}
	defer r.StopAll()
	require.Len(t, r.Status().Running, 2)

	// Stop one; the other keeps running.
	doJSON(t, http.MethodPost, ts.URL+"/api/stop", map[string]string{"name": "alpha"}).Body.Close()
	running := r.Status().Running
	require.Len(t, running, 1)
	assert.Equal(t, "beta", running[0].Name)
}

func TestServer_MonitorFollowsCurrent(t *testing.T) {
	ts, _ := cpServer(t)
	resp := doJSON(t, http.MethodPost, ts.URL+"/api/jobs", cpJob("alpha"))
	resp.Body.Close()

	// Idle: graph is empty.
	resp = doJSON(t, http.MethodGet, ts.URL+"/api/graph", nil)
	var nodes []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&nodes))
	resp.Body.Close()
	assert.Empty(t, nodes)

	// Running: graph reflects the live pipeline's stages.
	resp = doJSON(t, http.MethodPost, ts.URL+"/api/run", map[string]string{"name": "alpha"})
	resp.Body.Close()
	defer func() {
		doJSON(t, http.MethodPost, ts.URL+"/api/stop", map[string]string{"name": "alpha"}).Body.Close()
	}()

	resp = doJSON(t, http.MethodGet, ts.URL+"/api/graph?job=alpha", nil)
	nodes = nil
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&nodes))
	resp.Body.Close()
	assert.NotEmpty(t, nodes)
}

func TestServer_Sample(t *testing.T) {
	ts, _ := cpServer(t)
	resp := doJSON(t, http.MethodPost, ts.URL+"/api/jobs", cpJob("alpha"))
	resp.Body.Close()
	resp = doJSON(t, http.MethodPost, ts.URL+"/api/run", map[string]string{"name": "alpha"})
	resp.Body.Close()
	defer func() {
		doJSON(t, http.MethodPost, ts.URL+"/api/stop", map[string]string{"name": "alpha"}).Body.Close()
	}()

	// Poll briefly for the tap to capture records emitted by the "tag" stage.
	var records []core.Record
	for range 50 {
		resp = doJSON(t, http.MethodGet, ts.URL+"/api/sample?job=alpha&stage=tag", nil)
		var res struct {
			Stage   string        `json:"stage"`
			Records []core.Record `json:"records"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&res))
		resp.Body.Close()
		assert.Equal(t, "tag", res.Stage)
		if len(res.Records) > 0 {
			records = res.Records
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NotEmpty(t, records, "tap should capture stage output")
	assert.Contains(t, records[0].Payload, "x") // map-set field
}

func TestServer_EventsIdleEmitsState(t *testing.T) {
	ts, _ := cpServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/events", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	buf := make([]byte, 256)
	n, _ := resp.Body.Read(buf)
	assert.Contains(t, string(buf[:n]), `"state":"idle"`)
}

func TestServer_Auth(t *testing.T) {
	// Fail-open: no token configured.
	tsOpen, _ := cpServer(t)
	resp := doJSON(t, http.MethodGet, tsOpen.URL+"/api/jobs", nil)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Fail-closed: token required.
	ts, _ := cpServer(t, WithAuth("secret"))

	// Missing token → 401.
	resp = doJSON(t, http.MethodGet, ts.URL+"/api/jobs", nil)
	resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	// Bearer token → OK.
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/jobs", nil)
	req.Header.Set("Authorization", "Bearer secret")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Query token (for SSE) → OK.
	resp = doJSON(t, http.MethodGet, ts.URL+"/api/jobs?token=secret", nil)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Health probe exempt even without token.
	resp = doJSON(t, http.MethodGet, ts.URL+"/healthz", nil)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Wrong token → 401.
	resp = doJSON(t, http.MethodGet, ts.URL+"/api/jobs?token=nope", nil)
	resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}
