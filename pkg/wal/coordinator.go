package wal

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/gribovan2005/drift/pkg/checkpoint"
	"github.com/gribovan2005/drift/pkg/core"
)

// keyPrefix is the delivery-key namespace stamped by the WAL source.
const keyPrefix = "wal:"

// seenStoreID is the checkpoint key under which the idempotent sink persists its
// durable set of written delivery keys.
const seenStoreID = "wal-sink-seen"

// Coordinator owns a Log and hands out a matched WAL source and idempotent sink.
// Because Drift is single-process the sink's ack reaches the same Log the source
// reads from via a direct reference, not an RPC.
type Coordinator struct {
	log  *Log
	seen checkpoint.Store
}

// NewCoordinator wires log (durability + replay) to seen (the idempotent sink's
// durable seen-set of delivery keys).
func NewCoordinator(log *Log, seen checkpoint.Store) *Coordinator {
	return &Coordinator{log: log, seen: seen}
}

// Source wraps inner so every emitted record is appended to the log (and stamped
// with a stable DeliveryKey) before it enters the pipeline, and any un-committed
// log entries are replayed first on restart.
func (c *Coordinator) Source(inner core.Source) core.Source {
	return &walSource{c: c, inner: inner}
}

// Sink wraps inner with an idempotent, ack-on-write decorator: duplicates
// (records whose DeliveryKey is already in the durable seen-set) are skipped, and
// the log's commit watermark advances only after a record is written.
func (c *Coordinator) Sink(inner core.Sink) core.Sink {
	return &idempotentSink{c: c, inner: inner}
}

// ── source ──────────────────────────────────────────────────────────────────

type walSource struct {
	c     *Coordinator
	inner core.Source
}

func (s *walSource) Read(ctx context.Context) (<-chan core.Record, error) {
	replay, err := s.c.log.Uncommitted()
	if err != nil {
		return nil, fmt.Errorf("wal source: replay: %w", err)
	}

	innerCh, err := s.inner.Read(ctx)
	if err != nil {
		return nil, err
	}

	out := make(chan core.Record, 256)
	go func() {
		defer close(out)

		// 1. Replay un-committed entries first, preserving their original keys.
		for _, e := range replay {
			var rec core.Record
			if err := json.Unmarshal(e.Data, &rec); err != nil {
				continue // skip a corrupt frame rather than block replay
			}
			rec.DeliveryKey = keyFor(e.LSN)
			select {
			case out <- rec:
			case <-ctx.Done():
				return
			}
		}

		// 2. Drain the underlying source, appending each record before emitting.
		for {
			select {
			case rec, ok := <-innerCh:
				if !ok {
					return
				}
				rec.DeliveryKey = "" // never persist a stale key
				data, err := json.Marshal(rec)
				if err != nil {
					continue
				}
				lsn, err := s.c.log.Append(data)
				if err != nil {
					return // durability failed → stop emitting rather than risk loss
				}
				rec.DeliveryKey = keyFor(lsn)
				select {
				case out <- rec:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// ── sink ────────────────────────────────────────────────────────────────────

type idempotentSink struct {
	c     *Coordinator
	inner core.Sink
}

func (s *idempotentSink) Write(ctx context.Context, ch <-chan core.Record) error {
	seenLSN, err := s.loadSeen()
	if err != nil {
		return fmt.Errorf("idempotent sink: load seen-set: %w", err)
	}

	// Forward deduped records to the inner sink via its own channel; inner.Write
	// runs concurrently and consumes them.
	fwd := make(chan core.Record, 256)
	innerErr := make(chan error, 1)
	go func() { innerErr <- s.inner.Write(ctx, fwd) }()

	var loopErr error
loop:
	for {
		select {
		case rec, ok := <-ch:
			if !ok {
				break loop
			}
			lsn, hasLSN := parseLSN(rec.DeliveryKey)
			if hasLSN && seenLSN[lsn] {
				continue // duplicate (replay) → skip
			}
			select {
			case fwd <- rec:
			case <-ctx.Done():
				break loop
			}
			if hasLSN {
				seenLSN[lsn] = true
				if err := s.persistSeen(seenLSN); err != nil {
					loopErr = fmt.Errorf("idempotent sink: persist seen-set: %w", err)
					break loop
				}
				if err := s.c.log.Commit(contiguous(seenLSN)); err != nil {
					loopErr = fmt.Errorf("idempotent sink: commit: %w", err)
					break loop
				}
			}
		case <-ctx.Done():
			break loop
		}
	}

	close(fwd)
	if e := <-innerErr; e != nil {
		return e
	}
	return loopErr
}

// ── durable seen-set (LSN set persisted as JSON) ────────────────────────────

func (s *idempotentSink) loadSeen() (map[uint64]bool, error) {
	data, found, err := s.c.seen.Load(seenStoreID)
	if err != nil {
		return nil, err
	}
	seen := map[uint64]bool{}
	if !found {
		return seen, nil
	}
	var lsns []uint64
	if err := json.Unmarshal(data, &lsns); err != nil {
		return nil, err
	}
	for _, l := range lsns {
		seen[l] = true
	}
	return seen, nil
}

func (s *idempotentSink) persistSeen(seen map[uint64]bool) error {
	lsns := make([]uint64, 0, len(seen))
	for l := range seen {
		lsns = append(lsns, l)
	}
	data, err := json.Marshal(lsns)
	if err != nil {
		return err
	}
	return s.c.seen.Save(seenStoreID, data)
}

// contiguous returns the highest LSN N such that 1..N are all in seen.
func contiguous(seen map[uint64]bool) uint64 {
	var n uint64
	for seen[n+1] {
		n++
	}
	return n
}

// ── delivery-key helpers ────────────────────────────────────────────────────

func keyFor(lsn uint64) string { return keyPrefix + strconv.FormatUint(lsn, 10) }

func parseLSN(key string) (uint64, bool) {
	rest, ok := strings.CutPrefix(key, keyPrefix)
	if !ok {
		return 0, false
	}
	lsn, err := strconv.ParseUint(rest, 10, 64)
	if err != nil {
		return 0, false
	}
	return lsn, true
}
