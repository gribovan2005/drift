package job

// This file is the single source of truth for the visual builder's block palette
// and param forms (served at GET /api/palette). It MUST stay in sync with the
// buildSource/buildOperator/buildSink switches in components.go and builtins.go;
// the parity tests TestCatalog_CoversAllOps and TestCatalog_DefaultsLoad enforce
// that. See drift/Specs/Web UI & Builder.md.

// Param kinds understood by the builder's form renderer.
const (
	KindString   = "string"
	KindInt      = "int"
	KindNumber   = "number"
	KindDuration = "duration"
	KindBool     = "bool"
	KindEnum     = "enum"
	KindMap      = "map"
	KindAny      = "any"
)

// Param describes one configurable parameter of a block.
type Param struct {
	Name     string   `json:"name"`
	Kind     string   `json:"kind"`
	Required bool     `json:"required"`
	Default  any      `json:"default,omitempty"`
	Enum     []string `json:"enum,omitempty"`
	Doc      string   `json:"doc,omitempty"`
}

// BlockDef describes one palette block (a source, operator, or sink type).
type BlockDef struct {
	Kind           string  `json:"kind"` // "source" | "operator" | "sink"
	Type           string  `json:"type"` // the YAML type/op value
	Params         []Param `json:"params"`
	Flusher        bool    `json:"flusher,omitempty"`
	Parallelizable bool    `json:"parallelizable,omitempty"` // supports stage parallelism
	Doc            string  `json:"doc,omitempty"`
}

// Palette is the full catalog of built-in blocks.
type Palette struct {
	Sources   []BlockDef `json:"sources"`
	Operators []BlockDef `json:"operators"`
	Sinks     []BlockDef `json:"sinks"`
}

// Catalog returns the built-in block palette. ref:<name> blocks are intentionally
// excluded — they are host-registered and not configurable from the UI.
func Catalog() Palette {
	return Palette{
		Sources: []BlockDef{
			{Kind: "source", Type: "generator", Doc: "Emit synthetic records at a fixed rate.", Params: []Param{
				{Name: "rate", Kind: KindDuration, Required: true, Doc: "Interval between records, e.g. \"100ms\"."},
				{Name: "fields", Kind: KindMap, Doc: "Templates: seq · ${seq} · rand:int:1:100 · rand:float:0:1 · choice:a|b|c · else verbatim."},
			}},
			{Kind: "source", Type: "memory", Doc: "Empty in-process source (records supplied programmatically)."},
			{Kind: "source", Type: "http", Doc: "Accept records via HTTP POST.", Params: []Param{
				{Name: "addr", Kind: KindString, Required: true, Doc: "Listen address, e.g. \":8081\"."},
			}},
		},
		Operators: []BlockDef{
			{Kind: "operator", Type: "filter", Parallelizable: true, Doc: "Keep records where field <op> value.", Params: []Param{
				{Name: "field", Kind: KindString, Required: true, Doc: "Payload field to test."},
				{Name: "cmp", Kind: KindEnum, Required: true, Default: "gte", Enum: []string{"gte", "lte", "eq"}, Doc: "Comparison: ≥, ≤, or exact match."},
				{Name: "value", Kind: KindAny, Required: true, Doc: "Value to compare against (number for gte/lte)."},
			}},
			{Kind: "operator", Type: "map-set", Parallelizable: true, Doc: "Set a field to a constant value.", Params: []Param{
				{Name: "field", Kind: KindString, Required: true, Doc: "Field to set."},
				{Name: "value", Kind: KindAny, Required: true, Doc: "Value to assign."},
			}},
			{Kind: "operator", Type: "map-rename", Parallelizable: true, Doc: "Rename a payload field.", Params: []Param{
				{Name: "from", Kind: KindString, Required: true, Doc: "Existing field name."},
				{Name: "to", Kind: KindString, Required: true, Doc: "New field name."},
			}},
			{Kind: "operator", Type: "dedup", Parallelizable: true, Doc: "Drop repeat records by key within a window.", Params: []Param{
				{Name: "key", Kind: KindString, Required: true, Doc: "Payload field used as the dedup key."},
				{Name: "window", Kind: KindDuration, Default: "0s", Doc: "Dedup window; 0 disables."},
			}},
			{Kind: "operator", Type: "tumbling", Flusher: true, Doc: "Fixed-count window aggregation.", Params: []Param{
				{Name: "size", Kind: KindInt, Required: true, Doc: "Records per window (≥1)."},
				{Name: "agg", Kind: KindString, Default: "count", Doc: "\"count\" or \"sum:<field>\"."},
			}},
			{Kind: "operator", Type: "timestamp", Parallelizable: true, Doc: "Assign event time from a payload field.", Params: []Param{
				{Name: "field", Kind: KindString, Required: true, Doc: "Field holding time.Time / RFC3339 / Unix seconds."},
			}},
			{Kind: "operator", Type: "eventwindow", Flusher: true, Doc: "Event-time tumbling window with watermark.", Params: []Param{
				{Name: "size", Kind: KindDuration, Required: true, Doc: "Window duration, e.g. \"10s\"."},
				{Name: "lateness", Kind: KindDuration, Default: "0s", Doc: "Allowed out-of-orderness."},
				{Name: "agg", Kind: KindString, Default: "count", Doc: "\"count\" or \"sum:<field>\"."},
			}},
			{Kind: "operator", Type: "session", Flusher: true, Parallelizable: true, Doc: "Gap-based session window per key.", Params: []Param{
				{Name: "key", Kind: KindString, Required: true, Doc: "Payload field to session on."},
				{Name: "gap", Kind: KindDuration, Required: true, Doc: "Inactivity gap that closes a session."},
				{Name: "agg", Kind: KindString, Default: "count", Doc: "\"count\" or \"sum:<field>\"."},
			}},
		},
		Sinks: []BlockDef{
			{Kind: "sink", Type: "memory", Doc: "Collect records in process (inspect via API)."},
			{Kind: "sink", Type: "http", Doc: "POST each record to a URL.", Params: []Param{
				{Name: "url", Kind: KindString, Required: true, Doc: "Destination endpoint."},
			}},
		},
	}
}
