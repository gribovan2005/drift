package operator

import (
	"testing"

	"github.com/andrejgribov/drift/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func routeByOdd(r core.Record) int {
	n, _ := r.Payload["n"].(int)
	if n%2 == 0 {
		return 1
	}
	return 0
}

func TestSplit_HappyPath(t *testing.T) {
	s, err := NewSplit(2, routeByOdd, 100)
	require.NoError(t, err)

	in := []core.Record{recN(1), recN(2), recN(3), recN(4)}
	primary, err := s.Process(in)
	require.NoError(t, err)

	// Odd → route 0 (primary), Even → route 1 (side)
	assert.Len(t, primary, 2)
	assert.Equal(t, 1, primary[0].Payload["n"])
	assert.Equal(t, 3, primary[1].Payload["n"])

	side := s.Outputs()[0]
	assert.Len(t, side, 2)
	r1 := <-side
	r2 := <-side
	assert.Equal(t, 2, r1.Payload["n"])
	assert.Equal(t, 4, r2.Payload["n"])
}

func TestSplit_InvalidN(t *testing.T) {
	_, err := NewSplit(1, func(r core.Record) int { return 0 }, 10)
	assert.Error(t, err)

	_, err = NewSplit(0, func(r core.Record) int { return 0 }, 10)
	assert.Error(t, err)
}

func TestSplit_AllToRoute0(t *testing.T) {
	s, err := NewSplit(2, func(r core.Record) int { return 0 }, 10)
	require.NoError(t, err)

	primary, err := s.Process([]core.Record{recN(1), recN(2)})
	require.NoError(t, err)
	assert.Len(t, primary, 2)
	assert.Empty(t, s.Outputs()[0])
}

func TestSplit_OutOfRangeClampedTo0(t *testing.T) {
	s, err := NewSplit(2, func(r core.Record) int { return 99 }, 10)
	require.NoError(t, err)

	primary, err := s.Process([]core.Record{recN(1), recN(2)})
	require.NoError(t, err)
	assert.Len(t, primary, 2)
}

func TestSplit_NRoutes(t *testing.T) {
	router := func(r core.Record) int {
		n, _ := r.Payload["n"].(int)
		return n % 4
	}
	s, err := NewSplit(4, router, 100)
	require.NoError(t, err)

	in := make([]core.Record, 8)
	for i := range in {
		in[i] = recN(i)
	}
	primary, err := s.Process(in)
	require.NoError(t, err)

	// Route 0: n%4==0 → 0,4
	assert.Len(t, primary, 2)
	// Route 1: n%4==1 → 1,5
	assert.Len(t, s.Outputs()[0], 2)
	// Route 2: n%4==2 → 2,6
	assert.Len(t, s.Outputs()[1], 2)
	// Route 3: n%4==3 → 3,7
	assert.Len(t, s.Outputs()[2], 2)
}

func TestSplit_Close(t *testing.T) {
	s, err := NewSplit(3, func(r core.Record) int { return 0 }, 10)
	require.NoError(t, err)

	s.Close()

	_, ok := <-s.Outputs()[0]
	assert.False(t, ok, "side channel 0 must be closed")
	_, ok = <-s.Outputs()[1]
	assert.False(t, ok, "side channel 1 must be closed")
}

func TestSplit_OnSchemaChange_Concurrent(t *testing.T) {
	s, err := NewSplit(2, func(r core.Record) int { return 0 }, 1000)
	require.NoError(t, err)
	schema := core.Schema{ID: "s", Version: 1}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			s.OnSchemaChange(schema)
		}
	}()
	for i := 0; i < 100; i++ {
		s.Process([]core.Record{recN(i)}) //nolint:errcheck
	}
	<-done
}
