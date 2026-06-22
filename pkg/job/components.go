package job

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/sink"
	"github.com/gribovan2005/drift/pkg/source"
)

// buildSource constructs a core.Source from a component spec. "ref:<name>"
// resolves a host-registered source.
func buildSource(c ComponentSpec) (core.Source, error) {
	if name, ok := strings.CutPrefix(c.Type, "ref:"); ok {
		return lookupSource(name)
	}

	p := params(c.Params)
	switch c.Type {
	case "generator":
		rate, err := p.dur("rate")
		if err != nil {
			return nil, err
		}
		fields, _ := c.Params["fields"].(map[string]any)
		return source.NewGenerator(generatorFn(fields), rate), nil
	case "memory":
		// Records are supplied programmatically; YAML yields an empty source.
		return source.NewMemory(nil), nil
	case "http":
		addr, err := p.str("addr")
		if err != nil {
			return nil, err
		}
		return source.NewHTTP(addr), nil
	default:
		return nil, fmt.Errorf("unknown source type %q", c.Type)
	}
}

// buildSink constructs a core.Sink from a component spec.
func buildSink(c ComponentSpec) (core.Sink, error) {
	if name, ok := strings.CutPrefix(c.Type, "ref:"); ok {
		return lookupSink(name)
	}

	p := params(c.Params)
	switch c.Type {
	case "memory":
		return sink.NewMemory(), nil
	case "http":
		url, err := p.str("url")
		if err != nil {
			return nil, err
		}
		return sink.NewHTTP(url), nil
	default:
		return nil, fmt.Errorf("unknown sink type %q", c.Type)
	}
}

// generatorFn turns a YAML field template into a GeneratorFunc. For each field
// value (a string), the following templates are recognised:
//
//	seq                  → the numeric sequence value (0,1,2,…)
//	${seq}               → the sequence substituted into the string
//	rand:int:<min>:<max> → a pseudo-random integer in [min,max]
//	rand:float:<min>:<max> → a pseudo-random float in [min,max)
//	choice:a|b|c         → cycles through the options by sequence (a,b,c,a,…)
//	anything else        → used verbatim
func generatorFn(fields map[string]any) source.GeneratorFunc {
	return func(seq int) core.Record {
		payload := make(map[string]any, len(fields))
		for k, v := range fields {
			payload[k] = renderField(v, seq)
		}
		return core.Record{Payload: payload}
	}
}

func renderField(v any, seq int) any {
	s, ok := v.(string)
	if !ok {
		return v
	}
	switch {
	case s == "seq":
		return seq
	case strings.HasPrefix(s, "rand:int:"):
		lo, hi, ok := parseRange(strings.TrimPrefix(s, "rand:int:"))
		if !ok || hi < lo {
			return s
		}
		return int(lo) + rand.Intn(int(hi-lo)+1) //nolint:gosec
	case strings.HasPrefix(s, "rand:float:"):
		lo, hi, ok := parseRange(strings.TrimPrefix(s, "rand:float:"))
		if !ok || hi < lo {
			return s
		}
		return lo + rand.Float64()*(hi-lo) //nolint:gosec
	case strings.HasPrefix(s, "choice:"):
		opts := strings.Split(strings.TrimPrefix(s, "choice:"), "|")
		if len(opts) == 0 {
			return s
		}
		return opts[seq%len(opts)]
	case strings.Contains(s, "${seq}"):
		return strings.ReplaceAll(s, "${seq}", strconv.Itoa(seq))
	default:
		return s
	}
}

// parseRange parses "min:max" into two float64s.
func parseRange(s string) (lo, hi float64, ok bool) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	a, err1 := strconv.ParseFloat(parts[0], 64)
	b, err2 := strconv.ParseFloat(parts[1], 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return a, b, true
}
