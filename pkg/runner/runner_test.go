package runner_test

import (
	"testing"
	"time"

	"github.com/andrejgribov/drift/pkg/runner"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newRunner(t *testing.T, jobs ...string) *runner.Runner {
	t.Helper()
	s, err := runner.NewStore(t.TempDir())
	require.NoError(t, err)
	for _, name := range jobs {
		require.NoError(t, s.Save(genJob(name)))
	}
	return runner.New(s)
}

func runningNames(r *runner.Runner) []string {
	var out []string
	for _, j := range r.Status().Running {
		out = append(out, j.Name)
	}
	return out
}

func TestRunner_StartStopStatus(t *testing.T) {
	r := newRunner(t, "alpha")

	require.NoError(t, r.Start("alpha"))
	assert.Equal(t, []string{"alpha"}, runningNames(r))
	assert.NotNil(t, r.Current("alpha"))

	require.NoError(t, r.Stop("alpha"))
	assert.Empty(t, runningNames(r))
	assert.Nil(t, r.Current("alpha"))
}

func TestRunner_MultipleConcurrent(t *testing.T) {
	r := newRunner(t, "alpha", "beta")
	defer r.StopAll()

	require.NoError(t, r.Start("alpha"))
	require.NoError(t, r.Start("beta"))
	assert.Equal(t, []string{"alpha", "beta"}, runningNames(r))
	assert.NotNil(t, r.Current("alpha"))
	assert.NotNil(t, r.Current("beta"))

	require.NoError(t, r.Stop("alpha"))
	assert.Equal(t, []string{"beta"}, runningNames(r))
	assert.Nil(t, r.Current("alpha"))
	assert.NotNil(t, r.Current("beta"))
}

func TestRunner_RebuildPerRun(t *testing.T) {
	r := newRunner(t, "alpha")

	require.NoError(t, r.Start("alpha"))
	p1 := r.Current("alpha")
	require.NoError(t, r.Stop("alpha"))

	require.NoError(t, r.Start("alpha"))
	p2 := r.Current("alpha")
	require.NoError(t, r.Stop("alpha"))

	require.NotNil(t, p1)
	require.NotNil(t, p2)
	assert.NotSame(t, p1, p2, "each run must build a fresh pipeline")
}

func TestRunner_StartSameJobTwice(t *testing.T) {
	r := newRunner(t, "alpha")
	require.NoError(t, r.Start("alpha"))
	defer r.StopAll()

	assert.Error(t, r.Start("alpha"))
}

func TestRunner_StopWhenIdle(t *testing.T) {
	r := newRunner(t)
	assert.NoError(t, r.Stop("nope"))
}

func TestRunner_CurrentNilWhenIdle(t *testing.T) {
	r := newRunner(t, "alpha")
	assert.Nil(t, r.Current("alpha"))

	require.NoError(t, r.Start("alpha"))
	require.NoError(t, r.Stop("alpha"))
	assert.Nil(t, r.Current("alpha"))
}

func TestRunner_ConcurrentCurrentDuringSwap(t *testing.T) {
	r := newRunner(t, "alpha", "beta")

	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = r.Current("alpha")
				_ = r.Current("beta")
				_ = r.Status()
			}
		}
	}()

	for range 10 {
		require.NoError(t, r.Start("alpha"))
		require.NoError(t, r.Start("beta"))
		time.Sleep(time.Millisecond)
		require.NoError(t, r.Stop("alpha"))
		require.NoError(t, r.Stop("beta"))
	}
	close(stop)
}
