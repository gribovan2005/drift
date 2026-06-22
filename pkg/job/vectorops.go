package job

import (
	"fmt"
	"strings"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/vector"
)

// This file wires the columnar fast-lane (pkg/vector) into the declarative job
// registry. A YAML pipeline drops into the fast lane with a `to-batch` bridge
// (row→columnar), runs `vec-*` operators on chunk-records, and returns to the row
// path with `to-rows` before a row sink:
//
//	stages:
//	  - {label: batch, op: to-batch}
//	  - {label: agg,   op: vec-groupby, params: {key: merchant, agg: "count, sum:amount"}}
//	  - {label: rows,  op: to-rows}
//
// vec-* operators are single-stage (they keep per-key/window state) and reject
// stage parallelism — see parallelKey.

// vectorToRows is the columnar→row bridge (no params).
func vectorToRows() core.Operator { return vector.ToRows() }

func buildFromRows(p params) (core.Operator, error) {
	size := 0
	if p.has("size") {
		n, err := p.intVal("size")
		if err != nil {
			return nil, err
		}
		size = n
	}
	return vector.FromRows(size), nil
}

func buildVecFilter(p params) (core.Operator, error) {
	field, err := p.str("field")
	if err != nil {
		return nil, err
	}
	cmp := p.strOr("cmp", "gte")
	if cmp != "eq" && cmp != "gte" && cmp != "lte" {
		return nil, fmt.Errorf("vec-filter: cmp must be eq|gte|lte, got %q", cmp)
	}
	v, ok := p["value"]
	if !ok {
		return nil, fmt.Errorf("vec-filter: missing required param %q", "value")
	}
	switch n := v.(type) {
	case int:
		return vector.FilterInt64(field, intPred(cmp, int64(n))), nil
	case int64:
		return vector.FilterInt64(field, intPred(cmp, n)), nil
	case float64:
		return vector.FilterFloat64(field, floatPred(cmp, n)), nil
	default:
		return nil, fmt.Errorf("vec-filter: value must be a number (int or float), got %T", v)
	}
}

func intPred(cmp string, t int64) func(int64) bool {
	switch cmp {
	case "eq":
		return func(x int64) bool { return x == t }
	case "lte":
		return func(x int64) bool { return x <= t }
	default: // gte
		return func(x int64) bool { return x >= t }
	}
}

func floatPred(cmp string, t float64) func(float64) bool {
	switch cmp {
	case "eq":
		return func(x float64) bool { return x == t }
	case "lte":
		return func(x float64) bool { return x <= t }
	default: // gte
		return func(x float64) bool { return x >= t }
	}
}

func buildVecGroupBy(p params) (core.Operator, error) {
	key, err := p.str("key")
	if err != nil {
		return nil, err
	}
	decls, err := parseAggs(p.strOr("agg", "count"))
	if err != nil {
		return nil, err
	}
	g := vector.GroupBy(key)
	for _, d := range decls {
		switch d.kind {
		case "count":
			g = g.Count(d.out)
		case "sum":
			g = g.SumFloat64(d.field, d.out)
		case "sumi":
			g = g.SumInt64(d.field, d.out)
		case "max":
			g = g.MaxInt64(d.field, d.out)
		}
	}
	return g.Op(), nil
}

func buildVecTumbling(p params) (core.Operator, error) {
	key, ts, size, lateness, decls, err := windowParams(p)
	if err != nil {
		return nil, err
	}
	g := vector.TumblingGroup(key, ts, size).Lateness(lateness)
	applyWindowAggs(g, decls)
	return g.Op(), nil
}

func buildVecSliding(p params) (core.Operator, error) {
	key, ts, size, lateness, decls, err := windowParams(p)
	if err != nil {
		return nil, err
	}
	hop, err := int64Param(p, "hop")
	if err != nil {
		return nil, err
	}
	g := vector.SlidingGroup(key, ts, size, hop).Lateness(lateness)
	applyWindowAggs(g, decls)
	return g.Op(), nil
}

func buildVecSession(p params) (core.Operator, error) {
	key, err := p.str("key")
	if err != nil {
		return nil, err
	}
	ts, err := p.str("ts")
	if err != nil {
		return nil, err
	}
	gap, err := int64Param(p, "gap")
	if err != nil {
		return nil, err
	}
	lateness, _ := optInt64(p, "lateness")
	decls, err := parseAggs(p.strOr("agg", "count"))
	if err != nil {
		return nil, err
	}
	g := vector.SessionGroup(key, ts, gap).Lateness(lateness)
	for _, d := range decls {
		applySGroupAgg(g, d)
	}
	return g.Op(), nil
}

// windowParams pulls the shared tumbling/sliding params (key, ts, size, lateness, aggs).
func windowParams(p params) (key, ts string, size, lateness int64, decls []aggDecl, err error) {
	if key, err = p.str("key"); err != nil {
		return
	}
	if ts, err = p.str("ts"); err != nil {
		return
	}
	if size, err = int64Param(p, "size"); err != nil {
		return
	}
	lateness, _ = optInt64(p, "lateness")
	decls, err = parseAggs(p.strOr("agg", "count"))
	return
}

func applyWindowAggs(g *vector.WGroup, decls []aggDecl) {
	for _, d := range decls {
		switch d.kind {
		case "count":
			g.Count(d.out)
		case "sum":
			g.SumFloat64(d.field, d.out)
		case "sumi":
			g.SumInt64(d.field, d.out)
		case "max":
			g.MaxInt64(d.field, d.out)
		}
	}
}

func applySGroupAgg(g *vector.SGroup, d aggDecl) {
	switch d.kind {
	case "count":
		g.Count(d.out)
	case "sum":
		g.SumFloat64(d.field, d.out)
	case "sumi":
		g.SumInt64(d.field, d.out)
	case "max":
		g.MaxInt64(d.field, d.out)
	}
}

// aggDecl is one parsed aggregate: kind (count/sum/sumi/max), source field, out name.
type aggDecl struct {
	kind  string
	field string
	out   string
}

// parseAggs parses a comma-separated aggregate spec, e.g. "count, sum:amount, max:qty".
// Forms: "count" | "sum:<field>" (float64) | "sumi:<field>" (int64) | "max:<field>"
// (int64). Output column names: count→"count", else "<kind>_<field>".
func parseAggs(spec string) ([]aggDecl, error) {
	parts := strings.Split(spec, ",")
	out := make([]aggDecl, 0, len(parts))
	for _, raw := range parts {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if s == "count" {
			out = append(out, aggDecl{kind: "count", out: "count"})
			continue
		}
		kind, field, ok := strings.Cut(s, ":")
		if !ok || field == "" {
			return nil, fmt.Errorf("vec agg %q: want count | sum:<field> | sumi:<field> | max:<field>", s)
		}
		switch kind {
		case "sum", "sumi", "max":
			out = append(out, aggDecl{kind: kind, field: field, out: kind + "_" + field})
		default:
			return nil, fmt.Errorf("vec agg %q: unknown kind %q (want count|sum|sumi|max)", s, kind)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("vec agg: no aggregates parsed from %q", spec)
	}
	return out, nil
}

func int64Param(p params, key string) (int64, error) {
	n, err := p.intVal(key)
	if err != nil {
		return 0, err
	}
	return int64(n), nil
}

func optInt64(p params, key string) (int64, bool) {
	if !p.has(key) {
		return 0, false
	}
	n, err := p.intVal(key)
	if err != nil {
		return 0, false
	}
	return int64(n), true
}
