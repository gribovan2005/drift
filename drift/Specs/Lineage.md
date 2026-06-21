---
component: lineage
status: implemented
package: pkg/lineage
file: pkg/lineage/tracker.go
tested: true
---

# Lineage

Record-level provenance: for any output record, trace which input records it was
derived from, all the way back to the source. Complements the **static** lineage
of `drift graph` ([[CLI & Jobs]]) — that shows the stage DAG; this tracks actual
record flow at runtime.

---

## Data model

Two fields on `core.Record` carry identity and provenance:

```go
type Record struct {
    // ...
    ID      string   // unique per record instance; assigned by the tracker
    Parents []string // IDs of the records this one was derived from
}
```

- `ID`/`Parents` are empty when lineage is **off** — zero overhead, no behaviour
  change.
- A record's `ID` is minted **per stage**: the same logical datum gets a new ID
  after each operator, and `Parents` points at its immediate upstream IDs. This
  yields a step-by-step DAG (source → stage₁ → stage₂ → …) rather than a single
  mutating ID.

---

## Tracker

`pkg/lineage` owns the provenance graph.

```go
type Node struct {
    ID            string
    Stage         string   // operator label that produced the record ("source" for roots)
    SchemaID      string
    SchemaVersion int
    Parents       []string
}

func New() *Tracker
func (t *Tracker) Wrap(stage string, op core.Operator) core.Operator
func (t *Tracker) Get(id string) (Node, bool)
func (t *Tracker) Nodes() []Node
func (t *Tracker) Len() int
func (t *Tracker) Ancestors(id string) []Node // transitive parents, deduped
func (t *Tracker) Roots(id string) []Node     // source ancestors (no parents)
func (t *Tracker) Export() ([]byte, error)     // JSON of every node
```

`Tracker` is safe for concurrent use — stage goroutines record nodes in parallel.
IDs are a monotonic counter (`r1`, `r2`, …) so output is deterministic and tests
are stable.

---

## Wiring

`Wrap` returns an operator decorator that records nodes as records flow through.
It is applied to every stage via a pipeline option:

```go
t := lineage.New()
p := b.Pipeline(pipeline.WithLineage(t))
p.Run(ctx)
// ... inspect t.Ancestors(id), t.Export()
```

The decorator preserves the inner operator's optional interfaces: if the inner op
is a `Flusher` and/or `Snapshottable`, the wrapper exposes the same so windowing
and checkpointing keep working. The executor itself is **unchanged**.

---

## Attribution granularity

Parentage is **exact** for every built-in operator:

| Operator class | Detection | How parentage is captured |
|---|---|---|
| Stateless / stateful-passthrough (map, filter, flatmap, dedup, timestamp, schema-adapt) | not a `Flusher` | wrapper invokes the op **per record** → each output's parents are the single input it came from (1:1 or 1:N) |
| Aggregating windows (tumbling, sliding, eventwindow, session) | implements `Flusher` | the **operator** stamps each aggregate's `Parents` from the exact records in that window (`operator.withParents`); the wrapper preserves them |

How the window path stays exact: the wrapper assigns IDs to a stage's input
records **before** calling `Process`, so by the time a window buffers them they
already carry IDs. When the window fires, it sets the aggregate's `Parents` to
the IDs of precisely the records in that window — so two windows emitted from one
`Process` batch get disjoint parents, and records flushed at shutdown still carry
their true parents (the window retained them).

- Stateless ops are invoked per record inside the wrapper; this is safe because
  stateless ops are indifferent to batch boundaries.
- Fallback: if a custom `Flusher` declares no parents, its outputs are attributed
  to the whole input batch (`Process`) or recorded parentless (`Flush`).
- Under source fan-out (a record broadcast to multiple root stages), each branch
  mints its own root ID — branches are independent lineages by construction.

---

## Required tests

| Test | Proves |
|---|---|
| `TestTracker_AssignsRootIDs` | source records get IDs + root nodes |
| `TestTracker_MapExactParent` | map output parent = its single input |
| `TestTracker_FilterDropsHaveNoNode` | dropped records create no output node |
| `TestTracker_FlatMapMultipleChildren` | each child points at the one parent |
| `TestTracker_WindowExactParentsPerWindow` | two windows in one batch get disjoint, exact parents |
| `TestTracker_FlushOutputsCarryParents` | flushed partial windows carry their true parents |
| `TestTracker_Ancestors` | transitive walk source→…→output |
| `TestTracker_Concurrent` | `-race`: parallel stages recording |
| `TestTracker_Export` | JSON round-trips every node |
| `TestPipeline_Lineage_EndToEnd` | provenance through a real DAG pipeline |

---

## See also

- [[CLI & Jobs]] — static (stage-level) lineage via `drift graph`
- [[Core Abstractions]] — the `Record` type
- [[Operators]] — which operators are `Flusher`s
