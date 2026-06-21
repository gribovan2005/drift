package core

import (
	"context"
	"time"
)

// FieldType describes the allowed types for schema fields.
type FieldType string

const (
	FieldTypeString  FieldType = "string"
	FieldTypeInt     FieldType = "int"
	FieldTypeFloat   FieldType = "float"
	FieldTypeBool    FieldType = "bool"
	FieldTypeBytes   FieldType = "bytes"
	FieldTypeAny     FieldType = "any"
)

// Field is a single field descriptor in a Schema.
type Field struct {
	Name     string
	Type     FieldType
	Nullable bool
	Default  any // used by SchemaAdapter when adding new fields
}

// Schema describes the shape of Records flowing through a pipeline stage.
type Schema struct {
	ID      string
	Version int
	Fields  []Field
}

// FieldIndex returns the index of the named field, or -1 if not found.
func (s Schema) FieldIndex(name string) int {
	for i, f := range s.Fields {
		if f.Name == name {
			return i
		}
	}
	return -1
}

// Record is the fundamental unit of data in Drift.
// Payload keys are field names; values must match the field's FieldType.
type Record struct {
	SchemaID      string
	SchemaVersion int
	Payload       map[string]any

	// EventTime is when the event occurred at the source (event time), as
	// opposed to when Drift processed it (processing time). The zero value
	// means unset; populate it with operator.TimestampAssigner before any
	// event-time window. Event-time operators derive their watermark from it.
	EventTime time.Time

	// ID uniquely identifies this record instance for lineage tracking. It is
	// minted per stage by the lineage tracker; empty when lineage is disabled.
	ID string

	// Parents holds the IDs of the records this one was derived from. Empty for
	// source (root) records and when lineage is disabled. See pkg/lineage.
	Parents []string

	// DeliveryKey is a stable identifier used for exactly-once delivery: it is
	// identical when a record is replayed from the write-ahead log, so an
	// idempotent sink can recognise and skip duplicates. Set by the WAL source
	// (wal:<LSN>); empty when exactly-once is disabled. See pkg/wal.
	DeliveryKey string
}

// Operator transforms a batch of Records. Implementations must be safe
// for concurrent calls to Process from a single goroutine, but
// OnSchemaChange may be called from a different goroutine.
type Operator interface {
	// Process transforms input records into zero or more output records.
	Process(in []Record) ([]Record, error)

	// OnSchemaChange is called by SchemaRegistry when a new schema version
	// is published. The operator may use the new schema starting from the
	// next call to Process.
	OnSchemaChange(s Schema)
}

// Source produces Records. Read blocks until the first record is available
// or ctx is cancelled. The returned channel is closed when the source is
// exhausted or ctx is done.
type Source interface {
	Read(ctx context.Context) (<-chan Record, error)
}

// Sink consumes Records. Write blocks until all records from ch are consumed
// or ctx is cancelled.
type Sink interface {
	Write(ctx context.Context, ch <-chan Record) error
}

// Flusher is an optional interface for stateful operators (e.g. windows) that
// may hold buffered records. The pipeline calls Flush() after the upstream
// channel closes so that partial windows are emitted rather than dropped.
type Flusher interface {
	Flush() ([]Record, error)
}

// Snapshottable is an optional interface for stateful operators that can
// serialise and restore their internal state across pipeline restarts.
// Snapshot is called by the pipeline after all stage goroutines have exited
// (no concurrent Process calls). Restore is called before the first Process.
type Snapshottable interface {
	Snapshot() ([]byte, error)
	Restore([]byte) error
}
