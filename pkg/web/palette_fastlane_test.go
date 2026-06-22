package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gribovan2005/drift/pkg/job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPalette_IncludesFastLane confirms the visual builder palette (GET /api/palette)
// surfaces the columnar fast-lane operators, so they render as blocks in the UI.
func TestPalette_IncludesFastLane(t *testing.T) {
	s := testServer(t)
	rec := httptest.NewRecorder()
	s.handlePalette(rec, httptest.NewRequest(http.MethodGet, "/api/palette", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var pal job.Palette
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&pal))

	got := map[string]bool{}
	for _, b := range pal.Operators {
		got[b.Type] = true
	}
	for _, op := range []string{"to-batch", "to-rows", "vec-filter", "vec-map", "vec-groupby", "vec-tumbling", "vec-sliding", "vec-session"} {
		assert.True(t, got[op], "palette should include fast-lane op %q", op)
	}
}
