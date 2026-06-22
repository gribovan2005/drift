package sdk_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gribovan2005/drift/sdk"
)

func TestPrometheusHandler_EndToEnd(t *testing.T) {
	p, err := sdk.New().
		From(sdk.Slice(recs(10))).
		Map(func(r sdk.Record) (sdk.Record, error) { return r, nil }).
		To(sdk.Discard()).
		Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	srv := httptest.NewServer(sdk.PrometheusHandler(p))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	s := string(body)
	if !strings.Contains(s, "drift_stage_processed_total") {
		t.Fatalf("scrape missing stage metrics:\n%s", s)
	}
	if !strings.Contains(s, `stage="map-1"`) {
		t.Fatalf("scrape missing map-1 stage label:\n%s", s)
	}
}
