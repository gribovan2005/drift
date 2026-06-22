package job

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCatalog_CoversAllOps asserts the palette's type set matches the loader's
// build switches. If a new built-in is added to builtins.go/components.go without
// a catalog entry (or vice versa), this fails.
func TestCatalog_CoversAllOps(t *testing.T) {
	cat := Catalog()

	got := func(blocks []BlockDef) map[string]bool {
		m := map[string]bool{}
		for _, b := range blocks {
			m[b.Type] = true
		}
		return m
	}

	assert.Equal(t, map[string]bool{"generator": true, "memory": true, "http": true}, got(cat.Sources))
	assert.Equal(t, map[string]bool{
		"filter": true, "map-set": true, "map-rename": true, "dedup": true,
		"tumbling": true, "timestamp": true, "eventwindow": true, "session": true,
		"to-batch": true, "to-rows": true, "vec-filter": true, "vec-groupby": true,
		"vec-tumbling": true, "vec-sliding": true, "vec-session": true,
	}, got(cat.Operators))
	assert.Equal(t, map[string]bool{"memory": true, "http": true}, got(cat.Sinks))
}

// TestCatalog_DefaultsLoad builds a minimal spec for every catalog block (filling
// each declared param with its default or a kind-appropriate value) and asserts
// job.Load accepts it — proving the catalog never advertises a block the loader
// rejects.
func TestCatalog_DefaultsLoad(t *testing.T) {
	cat := Catalog()

	for _, b := range cat.Sources {
		t.Run("source/"+b.Type, func(t *testing.T) {
			spec := Spec{
				Name:   "t",
				Source: ComponentSpec{Type: b.Type, Params: synth(b)},
				Stages: []StageSpec{{Label: "s", Op: "map-set", Params: map[string]any{"field": "x", "value": 1}}},
				Sink:   ComponentSpec{Type: "memory"},
			}
			requireLoads(t, spec)
		})
	}

	for _, b := range cat.Operators {
		t.Run("operator/"+b.Type, func(t *testing.T) {
			spec := Spec{
				Name:   "t",
				Source: ComponentSpec{Type: "generator", Params: map[string]any{"rate": "1s"}},
				Stages: []StageSpec{{Label: "s", Op: b.Type, Params: synth(b)}},
				Sink:   ComponentSpec{Type: "memory"},
			}
			requireLoads(t, spec)
		})
	}

	for _, b := range cat.Sinks {
		t.Run("sink/"+b.Type, func(t *testing.T) {
			spec := Spec{
				Name:   "t",
				Source: ComponentSpec{Type: "generator", Params: map[string]any{"rate": "1s"}},
				Stages: []StageSpec{{Label: "s", Op: "map-set", Params: map[string]any{"field": "x", "value": 1}}},
				Sink:   ComponentSpec{Type: b.Type, Params: synth(b)},
			}
			requireLoads(t, spec)
		})
	}
}

func requireLoads(t *testing.T, spec Spec) {
	t.Helper()
	data, err := Marshal(spec)
	require.NoError(t, err)
	_, err = Load(data)
	require.NoError(t, err)
}

// synth fills every param of a block with its default or a kind-appropriate value.
func synth(b BlockDef) map[string]any {
	p := map[string]any{}
	for _, par := range b.Params {
		if par.Default != nil {
			p[par.Name] = par.Default
			continue
		}
		switch par.Kind {
		case KindString:
			p[par.Name] = "x"
		case KindInt:
			p[par.Name] = 1
		case KindNumber, KindAny:
			p[par.Name] = 1
		case KindDuration:
			p[par.Name] = "1s"
		case KindBool:
			p[par.Name] = true
		case KindEnum:
			if len(par.Enum) > 0 {
				p[par.Name] = par.Enum[0]
			}
		case KindMap:
			p[par.Name] = map[string]any{"id": "seq"}
		}
	}
	return p
}
