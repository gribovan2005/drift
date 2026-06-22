package sdk

import (
	"net/http"

	"github.com/gribovan2005/drift/pkg/metrics"
	"github.com/gribovan2005/drift/pkg/pipeline"
)

// PrometheusHandler returns an http.Handler that exposes p's per-stage metrics in
// the Prometheus text exposition format. Mount it in your service so its existing
// scraper can read the embedded pipeline:
//
//	p, _ := sdk.New().From(src).Map(fn).To(sink).Build()
//	http.Handle("/metrics", sdk.PrometheusHandler(p))
//	go p.Run(ctx)
func PrometheusHandler(p *pipeline.Pipeline) http.Handler {
	return metrics.Handler(p)
}
