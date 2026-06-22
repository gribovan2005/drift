package runner_test

import (
	"testing"

	"github.com/gribovan2005/drift/pkg/job"
	"github.com/gribovan2005/drift/pkg/runner"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func genJob(name string) job.Spec {
	return job.Spec{
		Name:   name,
		Source: job.ComponentSpec{Type: "generator", Params: map[string]any{"rate": "1ms", "fields": map[string]any{"id": "seq"}}},
		Stages: []job.StageSpec{{Label: "tag", Op: "map-set", Params: map[string]any{"field": "x", "value": 1}}},
		Sink:   job.ComponentSpec{Type: "memory"},
	}
}

func TestStore_SaveLoadRoundTrip(t *testing.T) {
	s, err := runner.NewStore(t.TempDir())
	require.NoError(t, err)

	require.NoError(t, s.Save(genJob("alpha")))

	got, err := s.Load("alpha")
	require.NoError(t, err)
	assert.Equal(t, "alpha", got.Name)
	assert.Equal(t, "generator", got.Source.Type)
	require.Len(t, got.Stages, 1)
	assert.Equal(t, "tag", got.Stages[0].Label)
}

func TestStore_ListDelete(t *testing.T) {
	s, err := runner.NewStore(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, s.Save(genJob("alpha")))
	require.NoError(t, s.Save(genJob("beta")))

	list, err := s.List()
	require.NoError(t, err)
	require.Len(t, list, 2)
	assert.Equal(t, "alpha", list[0].Name) // sorted
	assert.True(t, list[0].Valid)

	require.NoError(t, s.Delete("alpha"))
	list, err = s.List()
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "beta", list[0].Name)
}

func TestStore_Duplicate(t *testing.T) {
	s, err := runner.NewStore(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, s.Save(genJob("alpha")))

	require.NoError(t, s.Duplicate("alpha", "alpha-copy"))
	got, err := s.Load("alpha-copy")
	require.NoError(t, err)
	assert.Equal(t, "alpha-copy", got.Name)
}

func TestStore_ValidateBeforeSave(t *testing.T) {
	s, err := runner.NewStore(t.TempDir())
	require.NoError(t, err)

	bad := genJob("broken")
	bad.Stages[0].Op = "no-such-op"
	require.Error(t, s.Save(bad))

	// Nothing was written.
	list, err := s.List()
	require.NoError(t, err)
	assert.Empty(t, list)
}

func TestStore_PathTraversalRejected(t *testing.T) {
	s, err := runner.NewStore(t.TempDir())
	require.NoError(t, err)

	evil := genJob("alpha")
	evil.Name = "../evil"
	require.Error(t, s.Save(evil))

	_, err = s.Load("../../etc/passwd")
	require.Error(t, err)
	require.Error(t, s.Delete("../x"))
}
