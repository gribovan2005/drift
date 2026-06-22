package source

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
)

// HTTP listens on Addr and turns POST /ingest requests into a record stream.
// The request body must be a JSON array of core.Record objects.
// The source runs until ctx is cancelled.
type HTTP struct {
	Addr    string
	BufSize int // channel buffer; 0 → 256
}

// NewHTTP creates an HTTP source listening on addr.
func NewHTTP(addr string) *HTTP {
	return &HTTP{Addr: addr, BufSize: 256}
}

func (h *HTTP) Read(ctx context.Context) (<-chan core.Record, error) {
	buf := h.BufSize
	if buf == 0 {
		buf = 256
	}
	ch := make(chan core.Record, buf)

	ln, err := net.Listen("tcp", h.Addr)
	if err != nil {
		return nil, fmt.Errorf("http source listen %s: %w", h.Addr, err)
	}
	// Expose the actual address (useful when Addr is ":0").
	h.Addr = ln.Addr().String()

	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var records []core.Record
		if err := json.NewDecoder(r.Body).Decode(&records); err != nil {
			http.Error(w, fmt.Sprintf("decode: %v", err), http.StatusBadRequest)
			return
		}
		for _, rec := range records {
			select {
			case ch <- rec:
			case <-ctx.Done():
				http.Error(w, "shutting down", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Handler: mux, ReadTimeout: 10 * time.Second}

	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background()) //nolint:errcheck
		close(ch)
	}()
	go srv.Serve(ln) //nolint:errcheck

	return ch, nil
}
