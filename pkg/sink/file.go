package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/gribovan2005/drift/pkg/core"
)

// File writes each record as one JSON object per line (NDJSON) to a file. This
// is Drift's "log to storage" sink (Nexmark q10). Writes are buffered and
// flushed when the input channel closes.
type File struct {
	path string
}

// NewFile creates a File sink that writes to path (created/truncated on Write).
func NewFile(path string) *File { return &File{path: path} }

func (s *File) Write(ctx context.Context, ch <-chan core.Record) error {
	f, err := os.Create(s.path)
	if err != nil {
		return fmt.Errorf("file sink: create %s: %w", s.path, err)
	}
	defer f.Close() //nolint:errcheck

	w := bufio.NewWriterSize(f, 1<<16)
	enc := json.NewEncoder(w) // Encode writes a trailing newline → NDJSON
	for {
		select {
		case r, ok := <-ch:
			if !ok {
				return w.Flush()
			}
			if err := enc.Encode(r); err != nil {
				return fmt.Errorf("file sink: encode: %w", err)
			}
		case <-ctx.Done():
			return w.Flush()
		}
	}
}
