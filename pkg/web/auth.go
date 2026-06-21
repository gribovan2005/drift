package web

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// withAuth wraps h with token authentication. When the server has no token
// configured it is a pass-through (fail-open, for local/demo use). When a token
// is set, every request must carry it via `Authorization: Bearer <token>` or a
// `?token=<token>` query parameter (the latter for EventSource, which cannot set
// headers). The health probes are always exempt so orchestrators can reach them.
func (s *Server) withAuth(h http.Handler) http.Handler {
	if s.token == "" {
		return h
	}
	want := []byte(s.token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			h.ServeHTTP(w, r)
			return
		}
		if tokenOK(r, want) {
			h.ServeHTTP(w, r)
			return
		}
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
}

func tokenOK(r *http.Request, want []byte) bool {
	if h := r.Header.Get("Authorization"); h != "" {
		if tok, ok := strings.CutPrefix(h, "Bearer "); ok {
			return subtle.ConstantTimeCompare([]byte(tok), want) == 1
		}
	}
	if tok := r.URL.Query().Get("token"); tok != "" {
		return subtle.ConstantTimeCompare([]byte(tok), want) == 1
	}
	return false
}
