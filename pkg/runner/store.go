// Package runner is Drift's control plane: a filesystem-backed job store over a
// folder of YAML specs, plus a runner that builds, starts, stops, and reports the
// status of the currently running pipeline. See drift/Specs/Control Plane.md.
package runner

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	"github.com/gribovan2005/drift/pkg/job"
)

// nameRe restricts job names to filesystem-safe characters, guarding against
// path traversal (no slashes, dots, etc.).
var nameRe = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// JobInfo summarises a stored job for listing.
type JobInfo struct {
	Name    string    `json:"name"`
	ModTime time.Time `json:"mod_time"`
	Valid   bool      `json:"valid"`
	Error   string    `json:"error,omitempty"`
}

// Store persists job specs as <name>.yaml files under a directory.
type Store struct {
	dir string
}

// NewStore opens (creating if needed) a job store rooted at dir.
func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("runner: mkdir %s: %w", dir, err)
	}
	return &Store{dir: dir}, nil
}

// Dir returns the store's root directory.
func (s *Store) Dir() string { return s.dir }

func (s *Store) path(name string) (string, error) {
	if !nameRe.MatchString(name) {
		return "", fmt.Errorf("runner: invalid job name %q (allowed: letters, digits, _ and -)", name)
	}
	return filepath.Join(s.dir, name+".yaml"), nil
}

// List returns every stored job, sorted by name. A file that fails to parse is
// included with Valid=false and the error, so the UI can surface broken jobs.
func (s *Store) List() ([]JobInfo, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("runner: read dir: %w", err)
	}
	var out []JobInfo
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".yaml" {
			continue
		}
		name := e.Name()[:len(e.Name())-len(".yaml")]
		info := JobInfo{Name: name}
		if fi, err := e.Info(); err == nil {
			info.ModTime = fi.ModTime()
		}
		if data, err := os.ReadFile(filepath.Join(s.dir, e.Name())); err == nil {
			if _, err := job.Load(data); err != nil {
				info.Error = err.Error()
			} else {
				info.Valid = true
			}
		} else {
			info.Error = err.Error()
		}
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Load reads and parses a stored job's spec (without building components).
func (s *Store) Load(name string) (job.Spec, error) {
	path, err := s.path(name)
	if err != nil {
		return job.Spec{}, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return job.Spec{}, fmt.Errorf("runner: read %q: %w", name, err)
	}
	return job.Parse(data)
}

// Save validates a spec (a full job.Load dry run) and, only if it builds, writes
// it atomically as <name>.yaml. The name is taken from spec.Name.
func (s *Store) Save(spec job.Spec) error {
	if spec.Name == "" {
		return fmt.Errorf("runner: job spec missing name")
	}
	path, err := s.path(spec.Name)
	if err != nil {
		return err
	}
	data, err := job.Marshal(spec)
	if err != nil {
		return err
	}
	if _, err := job.Load(data); err != nil {
		return fmt.Errorf("runner: refusing to save invalid job: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("runner: write %q: %w", spec.Name, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("runner: rename %q: %w", spec.Name, err)
	}
	return nil
}

// Delete removes a stored job. Deleting a missing job is not an error.
func (s *Store) Delete(name string) error {
	path, err := s.path(name)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("runner: delete %q: %w", name, err)
	}
	return nil
}

// Duplicate copies an existing job under newName.
func (s *Store) Duplicate(name, newName string) error {
	spec, err := s.Load(name)
	if err != nil {
		return err
	}
	if !nameRe.MatchString(newName) {
		return fmt.Errorf("runner: invalid job name %q", newName)
	}
	spec.Name = newName
	return s.Save(spec)
}
