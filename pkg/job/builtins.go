package job

import (
	"fmt"
	"strings"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/operator"
	"github.com/gribovan2005/drift/pkg/pipeline"
)

// buildStageOp builds a stage's operator, applying intra-stage parallelism when
// StageSpec.Parallelism > 1: it constructs N fresh operator instances and wraps
// them in pipeline.Parallel with the right shard key.
func buildStageOp(s StageSpec) (core.Operator, error) {
	k := s.Parallelism
	if k <= 1 {
		return buildOperator(s)
	}
	key, err := parallelKey(s)
	if err != nil {
		return nil, err
	}
	ops := make([]core.Operator, k)
	for i := 0; i < k; i++ {
		op, err := buildOperator(s)
		if err != nil {
			return nil, err
		}
		ops[i] = op
	}
	return pipeline.Parallel(ops, key), nil
}

// parallelKey returns the shard-key func for a parallelizable op (nil = round-
// robin for stateless ops), or an error if the op cannot be safely parallelized.
func parallelKey(s StageSpec) (func(core.Record) string, error) {
	switch s.Op {
	case "filter", "map-set", "map-rename", "timestamp":
		return nil, nil // stateless → round-robin
	case "dedup", "session":
		kf := params(s.Params).strOr("key", "")
		if kf == "" {
			return nil, fmt.Errorf("%s: parallelism requires a key param", s.Op)
		}
		return fieldKey(kf), nil
	case "tumbling", "eventwindow":
		return nil, fmt.Errorf("%s is a global window with no partition key and cannot be parallelized (set parallelism: 1)", s.Op)
	default:
		return nil, fmt.Errorf("parallelism is only supported for built-in stateless or keyed operators, not %q", s.Op)
	}
}

// toFloat coerces a payload value to float64 for numeric comparison/aggregation.
func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

// aggregator parses an `agg` param ("count" or "sum:<field>") into an
// operator.AggregateFunc.
func aggregator(p params) (operator.AggregateFunc, error) {
	spec := p.strOr("agg", "count")
	switch {
	case spec == "count":
		return func(w []core.Record) (core.Record, error) {
			et := core.Record{}
			if len(w) > 0 {
				et.EventTime = w[0].EventTime
			}
			et.Payload = map[string]any{"count": len(w)}
			return et, nil
		}, nil
	case strings.HasPrefix(spec, "sum:"):
		field := strings.TrimPrefix(spec, "sum:")
		if field == "" {
			return nil, fmt.Errorf("agg sum: missing field name")
		}
		return func(w []core.Record) (core.Record, error) {
			var sum float64
			for _, r := range w {
				if f, ok := toFloat(r.Payload[field]); ok {
					sum += f
				}
			}
			out := core.Record{Payload: map[string]any{"sum": sum}}
			if len(w) > 0 {
				out.EventTime = w[0].EventTime
			}
			return out, nil
		}, nil
	default:
		return nil, fmt.Errorf("unknown agg %q (want \"count\" or \"sum:<field>\")", spec)
	}
}

// buildOperator constructs a core.Operator from a stage spec. "ref:<name>"
// resolves a host-registered operator; everything else is a built-in.
func buildOperator(s StageSpec) (core.Operator, error) {
	if name, ok := strings.CutPrefix(s.Op, "ref:"); ok {
		return lookupOp(name)
	}

	p := params(s.Params)
	switch s.Op {
	case "filter":
		return buildFilter(p)
	case "map-set":
		return buildMapSet(p)
	case "map-rename":
		return buildMapRename(p)
	case "dedup":
		return buildDedup(p)
	case "tumbling":
		return buildTumbling(p)
	case "timestamp":
		return buildTimestamp(p)
	case "eventwindow":
		return buildEventWindow(p)
	case "session":
		return buildSession(p)
	default:
		return nil, fmt.Errorf("unknown op %q", s.Op)
	}
}

func buildFilter(p params) (core.Operator, error) {
	field, err := p.str("field")
	if err != nil {
		return nil, err
	}

	// Friendly form: cmp + value (used by the visual builder). The comparison and
	// its operand live in two clear fields rather than three optional keys. The
	// key is "cmp" not "op" because "op" is a reserved stage field.
	if cmp := p.strOr("cmp", ""); cmp != "" {
		switch cmp {
		case "eq":
			want := p["value"]
			return operator.NewFilter(func(r core.Record) bool { return r.Payload[field] == want }), nil
		case "gte", "lte":
			threshold, err := p.num("value")
			if err != nil {
				return nil, err
			}
			return numFilter(field, cmp, threshold), nil
		default:
			return nil, fmt.Errorf("filter: unknown cmp %q (want eq/gte/lte)", cmp)
		}
	}

	// Legacy form: gte/lte/eq as direct keys (back-compat with hand-written YAML).
	cmp := p.firstOf("gte", "lte", "eq")
	if cmp == "" {
		return nil, fmt.Errorf("filter: need op+value, or one of gte/lte/eq")
	}
	if cmp == "eq" {
		want := p["eq"]
		return operator.NewFilter(func(r core.Record) bool { return r.Payload[field] == want }), nil
	}
	threshold, err := p.num(cmp)
	if err != nil {
		return nil, err
	}
	return numFilter(field, cmp, threshold), nil
}

// numFilter builds a numeric gte/lte filter on a payload field.
func numFilter(field, op string, threshold float64) core.Operator {
	return operator.NewFilter(func(r core.Record) bool {
		v, ok := toFloat(r.Payload[field])
		if !ok {
			return false
		}
		if op == "gte" {
			return v >= threshold
		}
		return v <= threshold
	})
}

func buildMapSet(p params) (core.Operator, error) {
	field, err := p.str("field")
	if err != nil {
		return nil, err
	}
	value := p["value"]
	return operator.NewMap(func(r core.Record) (core.Record, error) {
		out := cloneRecord(r)
		out.Payload[field] = value
		return out, nil
	}), nil
}

func buildMapRename(p params) (core.Operator, error) {
	from, err := p.str("from")
	if err != nil {
		return nil, err
	}
	to, err := p.str("to")
	if err != nil {
		return nil, err
	}
	return operator.NewMap(func(r core.Record) (core.Record, error) {
		out := cloneRecord(r)
		if v, ok := out.Payload[from]; ok {
			out.Payload[to] = v
			delete(out.Payload, from)
		}
		return out, nil
	}), nil
}

func buildDedup(p params) (core.Operator, error) {
	key, err := p.str("key")
	if err != nil {
		return nil, err
	}
	window, err := p.durOr("window", 0)
	if err != nil {
		return nil, err
	}
	return operator.NewDeduplicate(fieldKey(key), window), nil
}

func buildTumbling(p params) (core.Operator, error) {
	size, err := p.intVal("size")
	if err != nil {
		return nil, err
	}
	agg, err := aggregator(p)
	if err != nil {
		return nil, err
	}
	return operator.NewTumblingWindow(size, agg)
}

func buildTimestamp(p params) (core.Operator, error) {
	field, err := p.str("field")
	if err != nil {
		return nil, err
	}
	return operator.NewTimestampAssigner(fieldTime(field)), nil
}

func buildEventWindow(p params) (core.Operator, error) {
	size, err := p.dur("size")
	if err != nil {
		return nil, err
	}
	lateness, err := p.durOr("lateness", 0)
	if err != nil {
		return nil, err
	}
	agg, err := aggregator(p)
	if err != nil {
		return nil, err
	}
	return operator.NewEventTimeWindow(size, lateness, agg)
}

func buildSession(p params) (core.Operator, error) {
	key, err := p.str("key")
	if err != nil {
		return nil, err
	}
	gap, err := p.dur("gap")
	if err != nil {
		return nil, err
	}
	agg, err := aggregator(p)
	if err != nil {
		return nil, err
	}
	return operator.NewSessionWindow(gap, fieldKey(key), agg)
}

// fieldKey returns a KeyFunc that reads a payload field as a string key.
func fieldKey(field string) operator.KeyFunc {
	return func(r core.Record) string {
		return fmt.Sprintf("%v", r.Payload[field])
	}
}

// fieldTime returns a TimestampFunc that reads an event time from a payload
// field. It accepts time.Time, an RFC3339 string, or a number interpreted as
// Unix seconds. Unparseable values yield the zero time.
func fieldTime(field string) operator.TimestampFunc {
	return func(r core.Record) time.Time {
		switch v := r.Payload[field].(type) {
		case time.Time:
			return v
		case string:
			if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
				return t
			}
			return time.Time{}
		default:
			if secs, ok := toFloat(v); ok {
				return time.Unix(int64(secs), 0).UTC()
			}
			return time.Time{}
		}
	}
}

// cloneRecord makes a shallow copy with a fresh payload map so map operators do
// not mutate shared records (required under DAG fan-out).
func cloneRecord(r core.Record) core.Record {
	out := r
	out.Payload = make(map[string]any, len(r.Payload))
	for k, v := range r.Payload {
		out.Payload[k] = v
	}
	return out
}
