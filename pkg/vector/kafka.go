package vector

import (
	"context"

	"github.com/gribovan2005/drift/pkg/core"
	kafka "github.com/segmentio/kafka-go"
)

// KafkaColumnarSource reads a single partition of a topic whose messages are
// binary columnar frames (see EncodeBatch), decoding each to a chunk-record in the
// read path. Wrap N of these (one per partition) in source.NewParallel to consume
// partitions concurrently — the vectorized counterpart of source.KafkaPartitions.
// Frames that fail to decode are skipped.
func KafkaColumnarSource(brokers []string, topic string, partition int) core.Source {
	return &kafkaColSource{brokers: brokers, topic: topic, partition: partition}
}

type kafkaColSource struct {
	brokers   []string
	topic     string
	partition int
}

func (k *kafkaColSource) Read(ctx context.Context) (<-chan core.Record, error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     k.brokers,
		Topic:       k.topic,
		Partition:   k.partition,
		StartOffset: kafka.FirstOffset,
		MaxBytes:    10 * 1024 * 1024,
	})
	ch := make(chan core.Record, 16)
	go func() {
		defer close(ch)
		defer r.Close() //nolint:errcheck
		for {
			msg, err := r.ReadMessage(ctx)
			if err != nil {
				return // ctx cancelled or reader closed
			}
			b, err := DecodeBatch(msg.Value)
			if err != nil {
				continue
			}
			select {
			case ch <- core.Record{Chunk: b}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}
