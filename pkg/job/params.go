package job

import (
	"fmt"
	"time"
)

// params wraps a YAML param map with typed, validated accessors.
type params map[string]any

func (p params) has(key string) bool { _, ok := p[key]; return ok }

func (p params) str(key string) (string, error) {
	v, ok := p[key]
	if !ok {
		return "", fmt.Errorf("missing required param %q", key)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("param %q must be a string, got %T", key, v)
	}
	return s, nil
}

func (p params) strOr(key, def string) string {
	if s, err := p.str(key); err == nil {
		return s
	}
	return def
}

// num returns a numeric param as float64. YAML decodes ints as int and floats
// as float64, so accept both.
func (p params) num(key string) (float64, error) {
	v, ok := p[key]
	if !ok {
		return 0, fmt.Errorf("missing required param %q", key)
	}
	switch n := v.(type) {
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	case float64:
		return n, nil
	default:
		return 0, fmt.Errorf("param %q must be a number, got %T", key, v)
	}
}

func (p params) intVal(key string) (int, error) {
	f, err := p.num(key)
	if err != nil {
		return 0, err
	}
	return int(f), nil
}

// dur parses a duration param given as a Go duration string (e.g. "5s").
func (p params) dur(key string) (time.Duration, error) {
	s, err := p.str(key)
	if err != nil {
		return 0, err
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("param %q: invalid duration %q: %w", key, s, err)
	}
	return d, nil
}

func (p params) durOr(key string, def time.Duration) (time.Duration, error) {
	if !p.has(key) {
		return def, nil
	}
	return p.dur(key)
}

// firstOf returns the first present key among candidates, or "" if none.
func (p params) firstOf(candidates ...string) string {
	for _, k := range candidates {
		if p.has(k) {
			return k
		}
	}
	return ""
}
