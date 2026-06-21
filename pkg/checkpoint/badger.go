package checkpoint

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// BadgerStore persists operator state using an embedded BadgerDB database.
// Unlike FileStore (one file per key), BadgerStore uses a single LSM-tree
// directory, which is more efficient for many operators and survives restarts.
//
// Call Close after the pipeline stops to flush pending writes.
type BadgerStore struct {
	db *badger.DB
}

// NewBadgerStore opens (or creates) a BadgerDB database at dir.
func NewBadgerStore(dir string) (*BadgerStore, error) {
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: badger open %s: %w", dir, err)
	}
	return &BadgerStore{db: db}, nil
}

// Save persists data for id. Safe for concurrent use.
func (s *BadgerStore) Save(id string, data []byte) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(id), data)
	})
	if err != nil {
		return fmt.Errorf("checkpoint: badger save %s: %w", id, err)
	}
	return nil
}

// Load retrieves the stored blob for id.
// Returns found=false (not an error) when no checkpoint exists yet.
func (s *BadgerStore) Load(id string) ([]byte, bool, error) {
	var val []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(id))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("checkpoint: badger load %s: %w", id, err)
	}
	return val, true, nil
}

// Close flushes all pending writes and closes the database.
func (s *BadgerStore) Close() error {
	return s.db.Close()
}
