package job

import (
	"encoding/json"
	"fmt"
	"strings"
)

// graphEdge is a directed edge in the static lineage graph.
type graphEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// edges derives the static DAG from a built job, including the synthetic
// "source" and "sink" endpoints. Linear (unset) Next is resolved to the next
// stage in declaration order, mirroring pipeline execution.
func (b *Built) edges() []graphEdge {
	nodes := b.Pipeline().Graph() // Next already resolved to declaration order
	if len(nodes) == 0 {
		return nil
	}

	hasPred := make(map[string]bool)
	for _, n := range nodes {
		for _, nx := range n.Next {
			hasPred[nx] = true
		}
	}

	var es []graphEdge
	// source → every root stage (no predecessor)
	for _, n := range nodes {
		if !hasPred[n.Label] {
			es = append(es, graphEdge{From: "source", To: n.Label})
		}
	}
	// stage → next, and leaves → sink
	for _, n := range nodes {
		if len(n.Next) == 0 {
			es = append(es, graphEdge{From: n.Label, To: "sink"})
			continue
		}
		for _, nx := range n.Next {
			es = append(es, graphEdge{From: n.Label, To: nx})
		}
	}
	return es
}

// Graph renders the static lineage of a job in the given format:
// "mermaid" (default), "dot", or "json".
func (b *Built) Graph(format string) (string, error) {
	es := b.edges()
	switch format {
	case "", "mermaid":
		return renderMermaid(b.Spec.Name, es), nil
	case "dot":
		return renderDot(b.Spec.Name, es), nil
	case "json":
		out, err := json.MarshalIndent(struct {
			Name  string      `json:"name"`
			Edges []graphEdge `json:"edges"`
		}{b.Spec.Name, es}, "", "  ")
		if err != nil {
			return "", err
		}
		return string(out), nil
	default:
		return "", fmt.Errorf("unknown graph format %q (want mermaid|dot|json)", format)
	}
}

func renderMermaid(name string, es []graphEdge) string {
	var sb strings.Builder
	sb.WriteString("graph LR\n")
	for _, e := range es {
		fmt.Fprintf(&sb, "  %s --> %s\n", mermaidID(e.From), mermaidID(e.To))
	}
	return sb.String()
}

func renderDot(name string, es []graphEdge) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "digraph %q {\n", name)
	sb.WriteString("  rankdir=LR;\n")
	for _, e := range es {
		fmt.Fprintf(&sb, "  %q -> %q;\n", e.From, e.To)
	}
	sb.WriteString("}\n")
	return sb.String()
}

// mermaidID sanitizes a label into a mermaid-safe node id, preserving the label
// as the display text.
func mermaidID(label string) string {
	id := strings.NewReplacer("-", "_", ".", "_", " ", "_").Replace(label)
	return fmt.Sprintf("%s[%s]", id, label)
}
