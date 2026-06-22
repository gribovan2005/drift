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

// ParallelSource fans N sub-sources into one stream, read concurrently — use it to
// lift the single-reader ingestion ceiling. Cross-source order is not preserved.
func ParallelSource(subs ...Source) Source {
	return source.NewParallel(subs...)
}

// KafkaPartitions reads the given partitions of one topic in parallel (one
// partition-pinned reader each, fanned in). See source.KafkaPartitions.
func KafkaPartitions(cfg source.KafkaConfig, partitions []int) Source {
	return source.KafkaPartitions(cfg, partitions)
}
