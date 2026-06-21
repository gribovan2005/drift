package operator

import (
	"strconv"
	"testing"

	"github.com/andrejgribov/drift/pkg/core"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJoin_MatchesAcrossTypes(t *testing.T) {
	// Join auctions (by seller) with persons (by id): output person name + auction id.
	leftKey := func(r core.Record) string { return itoaP(r.Payload["seller"]) }
	rightKey := func(r core.Record) string { return itoaP(r.Payload["id"]) }
	j, err := NewJoin("auction", leftKey, "person", rightKey, 4,
		func(auction, person core.Record) (core.Record, error) {
			return core.Record{Payload: map[string]any{
				"name":    person.Payload["name"],
				"auction": auction.Payload["id"],
			}}, nil
		})
	require.NoError(t, err)

	// Person arrives first, then a matching auction → 1 match.
	out, err := j.Process([]core.Record{
		{SchemaID: "person", Payload: map[string]any{"id": 7, "name": "alice"}},
		{SchemaID: "auction", Payload: map[string]any{"id": 100, "seller": 7}},
		{SchemaID: "auction", Payload: map[string]any{"id": 101, "seller": 9}}, // no person 9 → no match
	})
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, "alice", out[0].Payload["name"])
	assert.Equal(t, 100, out[0].Payload["auction"])
}

func TestJoin_BothArrivalOrders(t *testing.T) {
	leftKey := func(r core.Record) string { return itoaP(r.Payload["seller"]) }
	rightKey := func(r core.Record) string { return itoaP(r.Payload["id"]) }
	j, _ := NewJoin("auction", leftKey, "person", rightKey, 4,
		func(a, p core.Record) (core.Record, error) {
			return core.Record{Payload: map[string]any{"ok": true}}, nil
		})

	// Auction first, then person → still matches (symmetric buffering).
	out, err := j.Process([]core.Record{
		{SchemaID: "auction", Payload: map[string]any{"id": 1, "seller": 3}},
		{SchemaID: "person", Payload: map[string]any{"id": 3, "name": "bob"}},
	})
	require.NoError(t, err)
	assert.Len(t, out, 1)
}

func TestJoin_SnapshotRestore(t *testing.T) {
	leftKey := func(r core.Record) string { return itoaP(r.Payload["seller"]) }
	rightKey := func(r core.Record) string { return itoaP(r.Payload["id"]) }
	mk := func() *Join {
		j, _ := NewJoin("auction", leftKey, "person", rightKey, 4,
			func(a, p core.Record) (core.Record, error) {
				return core.Record{Payload: map[string]any{"ok": true}}, nil
			})
		return j
	}
	j := mk()
	_, _ = j.Process([]core.Record{{SchemaID: "person", Payload: map[string]any{"id": 5, "name": "x"}}})
	blob, err := j.Snapshot()
	require.NoError(t, err)

	j2 := mk()
	require.NoError(t, j2.Restore(blob))
	out, err := j2.Process([]core.Record{{SchemaID: "auction", Payload: map[string]any{"id": 9, "seller": 5}}})
	require.NoError(t, err)
	assert.Len(t, out, 1, "restored person should still match")
}

func itoaP(v any) string {
	if n, ok := v.(int); ok {
		return strconv.Itoa(n)
	}
	return ""
}
