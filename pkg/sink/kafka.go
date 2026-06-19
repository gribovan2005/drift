package sink

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/andrejgribov/drift/pkg/core"
	kafka "github.com/segmentio/kafka-go"
)

// KafkaConfig holds connection parameters for a Kafka sink.
type KafkaConfig struct {
	Brokers []string
	Topic   string
	// Async enables fire-and-forget writes (higher throughput, no error feedback).
	Async bool
}

// Kafka writes each Record as a JSON message to a Kafka topic.
// Delivery guarantee: at-least-once.
type Kafka struct {
	cfg    KafkaConfig
	writer *kafka.Writer
}

// NewKafka creates a Kafka sink with the given config.
func NewKafka(cfg KafkaConfig) (*Kafka, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka sink: at least one broker required")
	}
	w := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Topic:                  cfg.Topic,
		AllowAutoTopicCreation: true,
		Async:                  cfg.Async,
	}
	return &Kafka{cfg: cfg, writer: w}, nil
}

func (k *Kafka) Write(ctx context.Context, ch <-chan core.Record) error {
	defer k.writer.Close() //nolint:errcheck
	for {
		select {
		case r, ok := <-ch:
			if !ok {
				return nil
			}
			body, err := json.Marshal(r)
			if err != nil {
				return fmt.Errorf("kafka sink marshal: %w", err)
			}
			if err := k.writer.WriteMessages(ctx, kafka.Message{Value: body}); err != nil {
				return fmt.Errorf("kafka sink write: %w", err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}
