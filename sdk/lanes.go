package sdk

import (
	"context"
	"fmt"

	"github.com/gribovan2005/drift/pkg/pipeline"
)

// RunLanes builds each Stream and runs them as independent lanes via
// pipeline.RunLanes (no shared channel) — the way to scale a single node toward
// linear. Each stream needs its own source shard and sink. A build error surfaces
// (with the lane index) before anything runs.
//
//	streams := make([]*sdk.Stream, n)
//	for i := range streams {
//	    streams[i] = sdk.New().From(shard(i)).Apply(vector.MapInt64("v", fn)).To(sink(i))
//	}
//	sdk.RunLanes(ctx, streams...)
func RunLanes(ctx context.Context, streams ...*Stream) error {
	lanes := make([]*pipeline.Pipeline, len(streams))
	for i, s := range streams {
		p, err := s.Build()
		if err != nil {
			return fmt.Errorf("lane %d: %w", i, err)
		}
		lanes[i] = p
	}
	return pipeline.RunLanes(ctx, lanes...)
}
