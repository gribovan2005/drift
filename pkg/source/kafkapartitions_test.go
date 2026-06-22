package source_test

import (
	"context"
	"testing"

	"github.com/gribovan2005/drift/pkg/source"
)

// TestKafkaPartitions_Wiring checks the partition fan-in wiring without a broker:
// each partition reader errors on no brokers, and Parallel propagates that error.
// (Live partition reads are covered by the KAFKA_ADDR-guarded Kafka tests.)
func TestKafkaPartitions_Wiring(t *testing.T) {
	src := source.KafkaPartitions(source.KafkaConfig{Topic: "t"}, []int{0, 1, 2})
	if src == nil {
		t.Fatal("KafkaPartitions returned nil")
	}
	if _, err := src.Read(context.Background()); err == nil {
		t.Fatal("expected error from partition reader with no brokers")
	}
}
