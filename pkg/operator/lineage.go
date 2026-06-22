package operator

import "github.com/gribovan2005/drift/pkg/core"

// withParents stamps an aggregate output with the lineage IDs of the records
// that produced it, giving aggregating operators exact (per-window, not
// per-batch) provenance. Record IDs are populated only when lineage tracking is
// enabled (see pkg/lineage); with lineage off no record carries an ID, so this
// leaves Parents nil and has no effect.
func withParents(agg core.Record, src []core.Record) core.Record {
	var parents []string
	for _, r := range src {
		if r.ID != "" {
			parents = append(parents, r.ID)
		}
	}
	agg.Parents = parents
	return agg
}
