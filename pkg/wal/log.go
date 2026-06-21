// Package wal implements a dependency-free write-ahead log and the coordinator
// that wires a WAL source to an idempotent sink for exactly-once delivery.
//
// The log is an append-only segment file: every Append is fsync'd before it
// returns, so a record that entered the pipeline is durably recoverable after a
// crash. A separate, atomically-written commit watermark records how far the
// sink has durably processed; on restart, Uncommitted replays everything past
// it. See drift/Specs/Exactly-Once.md.
package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// frameHeader is the on-disk prefix of every entry: u32 payload length + u64 LSN.
const frameHeader = 4 + 8

// Entry is a single logged record.
type Entry struct {
	LSN  uint64
	Data []byte
}

// Log is an append-only write-ahead log with a durable commit watermark.
type Log struct {
	mu        sync.Mutex
	dir       string
	f         *os.File
	nextLSN   uint64
	committed uint64
}

// Open creates dir if needed and opens (or creates) the log, recovering the
// next LSN by scanning the segment and the commit watermark from disk.
func Open(dir string) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %s: %w", dir, err)
	}
	l := &Log{dir: dir, nextLSN: 1}

	maxLSN, err := l.scan()
	if err != nil {
		return nil, err
	}
	l.nextLSN = maxLSN + 1

	committed, err := l.readWatermark()
	if err != nil {
		return nil, err
	}
	l.committed = committed

	f, err := os.OpenFile(l.logPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open log: %w", err)
	}
	l.f = f
	return l, nil
}

func (l *Log) logPath() string    { return filepath.Join(l.dir, "log.wal") }
func (l *Log) commitPath() string { return filepath.Join(l.dir, "commit") }

// scan walks the segment, returning the highest intact LSN. A torn trailing
// frame (a crash mid-append) is detected and ignored: scanning stops at the last
// complete frame.
func (l *Log) scan() (uint64, error) {
	f, err := os.Open(l.logPath())
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("wal: open for scan: %w", err)
	}
	defer f.Close() //nolint:errcheck

	var maxLSN uint64
	hdr := make([]byte, frameHeader)
	for {
		if _, err := io.ReadFull(f, hdr); err != nil {
			// EOF or a partial header → end of intact data.
			break
		}
		length := binary.LittleEndian.Uint32(hdr[:4])
		lsn := binary.LittleEndian.Uint64(hdr[4:])
		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			// Torn payload from a crash mid-append → stop at previous frame.
			break
		}
		maxLSN = lsn
	}
	return maxLSN, nil
}

// Append durably writes data and returns its assigned LSN.
func (l *Log) Append(data []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lsn := l.nextLSN
	hdr := make([]byte, frameHeader)
	binary.LittleEndian.PutUint32(hdr[:4], uint32(len(data)))
	binary.LittleEndian.PutUint64(hdr[4:], lsn)

	if _, err := l.f.Write(hdr); err != nil {
		return 0, fmt.Errorf("wal: write header: %w", err)
	}
	if _, err := l.f.Write(data); err != nil {
		return 0, fmt.Errorf("wal: write payload: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		return 0, fmt.Errorf("wal: fsync: %w", err)
	}
	l.nextLSN++
	return lsn, nil
}

// Commit persists lsn as the highest durably-processed entry. Commits never move
// the watermark backwards.
func (l *Log) Commit(lsn uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if lsn <= l.committed {
		return nil
	}
	if err := l.writeWatermark(lsn); err != nil {
		return err
	}
	l.committed = lsn
	return nil
}

// Committed returns the current commit watermark.
func (l *Log) Committed() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.committed
}

// Uncommitted returns every entry with LSN greater than the commit watermark,
// in append order.
func (l *Log) Uncommitted() ([]Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	f, err := os.Open(l.logPath())
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("wal: open for replay: %w", err)
	}
	defer f.Close() //nolint:errcheck

	var entries []Entry
	hdr := make([]byte, frameHeader)
	for {
		if _, err := io.ReadFull(f, hdr); err != nil {
			break
		}
		length := binary.LittleEndian.Uint32(hdr[:4])
		lsn := binary.LittleEndian.Uint64(hdr[4:])
		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			break // torn trailing frame
		}
		if lsn > l.committed {
			entries = append(entries, Entry{LSN: lsn, Data: payload})
		}
	}
	return entries, nil
}

// Close flushes and closes the underlying file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	return err
}

// writeWatermark atomically persists the commit watermark (.tmp + rename), so a
// crash mid-write leaves the previous watermark intact.
func (l *Log) writeWatermark(lsn uint64) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, lsn)
	tmp := l.commitPath() + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return fmt.Errorf("wal: write watermark: %w", err)
	}
	if err := os.Rename(tmp, l.commitPath()); err != nil {
		return fmt.Errorf("wal: rename watermark: %w", err)
	}
	return nil
}

// readWatermark reads the persisted commit watermark, or 0 if none/corrupt.
func (l *Log) readWatermark() (uint64, error) {
	buf, err := os.ReadFile(l.commitPath())
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("wal: read watermark: %w", err)
	}
	if len(buf) < 8 {
		return 0, nil // torn watermark → fall back to nothing committed
	}
	return binary.LittleEndian.Uint64(buf), nil
}
