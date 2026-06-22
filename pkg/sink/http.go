package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
)

// HTTP POSTs each record as a JSON object to URL.
type HTTP struct {
	URL    string
	client *http.Client
}

// NewHTTP creates an HTTP sink that forwards records to url.
func NewHTTP(url string) *HTTP {
	return &HTTP{
		URL:    url,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

func (h *HTTP) Write(ctx context.Context, ch <-chan core.Record) error {
	for {
		select {
		case r, ok := <-ch:
			if !ok {
				return nil
			}
			if err := h.post(ctx, r); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (h *HTTP) post(ctx context.Context, r core.Record) error {
	body, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("http sink marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("http sink request: %w", err)
	}
	req.Header.Set("content-type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("http sink post: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("http sink: upstream returned %d", resp.StatusCode)
	}
	return nil
}
