package sdk

import (
	"context"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/sink"
)

// Collector is an in-memory Sink that retains every record it receives. Read
// them with Records after Run returns.
type Collector struct {
	mem *sink.Memory
}

// Collect returns a fresh in-memory Collector sink.
func Collect() *Collector {
	return &Collector{mem: sink.NewMemory()}
}

// Write implements core.Sink.
func (c *Collector) Write(ctx context.Context, ch <-chan core.Record) error {
	return c.mem.Write(ctx, ch)
}

// Records returns a copy of the collected records. Safe to call after Run.
func (c *Collector) Records() []Record {
	return c.mem.Records()
}

// collectInto is a Sink that appends each arriving record into *dst.
type collectInto struct {
	dst *[]Record
}

// CollectInto returns a Sink that appends every record into *dst. The sink's
// single Write goroutine is the only writer, so no locking is needed; read *dst
// after Run returns.
func CollectInto(dst *[]Record) Sink {
	return &collectInto{dst: dst}
}

func (c *collectInto) Write(ctx context.Context, ch <-chan core.Record) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r, ok := <-ch:
			if !ok {
				return nil
			}
			*c.dst = append(*c.dst, r)
		}
	}
}

// discard is a Sink that drains and drops every record.
type discard struct{}

// Discard returns a Sink that consumes and drops all records — useful for load
// tests where only throughput matters.
func Discard() Sink { return discard{} }

func (discard) Write(ctx context.Context, ch <-chan core.Record) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-ch:
			if !ok {
				return nil
			}
		}
	}
}

// ToFile returns a Sink that writes each record as one JSON object per line
// (NDJSON) to path.
func ToFile(path string) Sink {
	return sink.NewFile(path)
}

// HTTPSink returns a Sink that POSTs each record as JSON to url.
func HTTPSink(url string) Sink {
	return sink.NewHTTP(url)
}

// KafkaSink returns a Kafka-backed Sink. See sink.KafkaConfig for fields.
func KafkaSink(cfg sink.KafkaConfig) (Sink, error) {
	return sink.NewKafka(cfg)
}
