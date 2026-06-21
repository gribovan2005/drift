// Package lineage tracks record-level provenance at runtime. A Tracker wraps
// each pipeline operator with a decorator that mints a per-stage ID for every
// record and records its parent IDs, building a provenance DAG that can be
// walked from any output record back to the source.
//
// Lineage is observational: it never changes operator behaviour. When a pipeline
// runs without a Tracker, Record.ID and Record.Parents stay empty and there is
// no overhead.
package lineage

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

// sourceStage is the synthetic stage name for root (source) records.
const sourceStage = "source"

// Node is one record instance in the provenance graph.
type Node struct {
	ID            string   `json:"id"`
	Stage         string   `json:"stage"`
	SchemaID      string   `json:"schema_id,omitempty"`
	SchemaVersion int      `json:"schema_version,omitempty"`
	Parents       []string `json:"parents,omitempty"`
}

// Tracker holds the provenance graph and mints record IDs. It is safe for
// concurrent use by multiple stage goroutines.
type Tracker struct {
	seq atomic.Uint64

	mu    sync.RWMutex
	nodes map[string]*Node
}

// New returns an empty Tracker.
func New() *Tracker {
	return &Tracker{nodes: map[string]*Node{}}
}

// nextID returns a fresh, monotonically increasing record ID.
func (t *Tracker) nextID() string {
	return fmt.Sprintf("r%d", t.seq.Add(1))
}

// add records a node in the graph.
func (t *Tracker) add(n *Node) {
	t.mu.Lock()
	t.nodes[n.ID] = n
	t.mu.Unlock()
}

// Get returns the node for an ID.
func (t *Tracker) Get(id string) (Node, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.nodes[id]
	if !ok {
		return Node{}, false
	}
	return *n, true
}

// Len returns the number of recorded nodes.
func (t *Tracker) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.nodes)
}

// Nodes returns every recorded node, ordered by ID for deterministic output.
func (t *Tracker) Nodes() []Node {
	t.mu.RLock()
	out := make([]Node, 0, len(t.nodes))
	for _, n := range t.nodes {
		out = append(out, *n)
	}
	t.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return less(out[i].ID, out[j].ID) })
	return out
}

// Ancestors returns the transitive parents of id (excluding id itself), deduped
// and ordered by ID.
func (t *Tracker) Ancestors(id string) []Node {
	t.mu.RLock()
	defer t.mu.RUnlock()

	seen := map[string]bool{}
	var out []Node
	var walk func(cur string)
	walk = func(cur string) {
		n, ok := t.nodes[cur]
		if !ok {
			return
		}
		for _, p := range n.Parents {
			if seen[p] {
				continue
			}
			seen[p] = true
			if pn, ok := t.nodes[p]; ok {
				out = append(out, *pn)
			}
			walk(p)
		}
	}
	walk(id)
	sort.Slice(out, func(i, j int) bool { return less(out[i].ID, out[j].ID) })
	return out
}

// Roots returns the source ancestors of id (nodes with no parents). If id is
// itself a root, it is returned.
func (t *Tracker) Roots(id string) []Node {
	t.mu.RLock()
	defer t.mu.RUnlock()

	seen := map[string]bool{}
	var roots []Node
	var walk func(cur string)
	walk = func(cur string) {
		if seen[cur] {
			return
		}
		seen[cur] = true
		n, ok := t.nodes[cur]
		if !ok {
			return
		}
		if len(n.Parents) == 0 {
			roots = append(roots, *n)
			return
		}
		for _, p := range n.Parents {
			walk(p)
		}
	}
	walk(id)
	sort.Slice(roots, func(i, j int) bool { return less(roots[i].ID, roots[j].ID) })
	return roots
}

// Export serialises every node as JSON.
func (t *Tracker) Export() ([]byte, error) {
	return json.MarshalIndent(t.Nodes(), "", "  ")
}

// less orders IDs of the form "r<n>" numerically, falling back to string order.
func less(a, b string) bool {
	na, oka := parseID(a)
	nb, okb := parseID(b)
	if oka && okb {
		return na < nb
	}
	return a < b
}

func parseID(s string) (uint64, bool) {
	if len(s) < 2 || s[0] != 'r' {
		return 0, false
	}
	var n uint64
	for _, c := range s[1:] {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + uint64(c-'0')
	}
	return n, true
}
