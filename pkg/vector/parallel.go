package vector

import (
	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/pipeline"
)

// Parallel runs n copies of a vectorized operator across n goroutines, round-
// robining whole chunk-records between them — use it to scale a vectorized stage
// past a single core when the per-row work is heavy. Each chunk goes to exactly
// one shard, so the in-place mutation stays safe.
//
// Only for STATELESS ops (Map/Filter): each shard keeps its own state, so wrapping
// an aggregate would yield per-shard partials. mk builds a fresh operator per shard.
func Parallel(n int, mk func() core.Operator) core.Operator {
	ops := make([]core.Operator, n)
	for i := range ops {
		ops[i] = mk()
	}
	return pipeline.Parallel(ops, nil)
}
