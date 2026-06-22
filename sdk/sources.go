package sdk

import (
	"time"

	"github.com/gribovan2005/drift/pkg/source"
)

// Slice returns a Source that emits the given records in order, then closes.
// Handy for tests and bounded batch jobs.
func Slice(records []Record) Source {
	return source.NewMemory(records)
}

// Generate returns a Source that calls fn(seq) to produce each record, emitting
// one every `every` (pass 0 for maximum throughput). It runs until the context
// is cancelled.
func Generate(fn func(seq int) Record, every time.Duration) Source {
	return source.NewGenerator(source.GeneratorFunc(fn), every)
}

// HTTPSource returns a Source that ingests records via POST /ingest on addr.
func HTTPSource(addr string) Source {
	return source.NewHTTP(addr)
}

// KafkaSource returns a Kafka-backed Source. See source.KafkaConfig for fields.
func KafkaSource(cfg source.KafkaConfig) Source {
	return source.NewKafka(cfg)
}
