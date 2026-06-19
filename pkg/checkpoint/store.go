// Package checkpoint persists and restores operator state across pipeline
// restarts. The FileStore implementation is dependency-free: state is written
// atomically (write to .tmp, rename) so a crash mid-write leaves the previous
// checkpoint intact.
package checkpoint

import (
	"fmt"
	"os"
	"path/filepath"
)

// Store saves and loads operator state blobs identified by a string key.
type Store interface {
	Save(id string, data []byte) error
	Load(id string) (data []byte, found bool, err error)
}

// FileStore persists each operator's state as a file under dir.
type FileStore struct {
	dir string
}

// NewFileStore creates a FileStore rooted at dir, creating it if necessary.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("checkpoint: mkdir %s: %w", dir, err)
	}
	return &FileStore{dir: dir}, nil
}

// Save writes data for id atomically. A crash between write and rename
// leaves the previous checkpoint file intact.
func (s *FileStore) Save(id string, data []byte) error {
	path := s.path(id)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("checkpoint: write %s: %w", id, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("checkpoint: rename %s: %w", id, err)
	}
	return nil
}

// Load returns the stored blob for id. Returns found=false (not an error)
// when no checkpoint exists yet.
func (s *FileStore) Load(id string) ([]byte, bool, error) {
	data, err := os.ReadFile(s.path(id))
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("checkpoint: read %s: %w", id, err)
	}
	return data, true, nil
}

func (s *FileStore) path(id string) string {
	return filepath.Join(s.dir, sanitize(id)+".ckpt")
}

// sanitize replaces non-alphanumeric characters with underscores so the
// operator label is safe to use as a filename on any OS.
func sanitize(id string) string {
	b := []byte(id)
	for i, c := range b {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' {
			continue
		}
		b[i] = '_'
	}
	return string(b)
}
