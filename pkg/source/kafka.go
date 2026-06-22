package source

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/dlq"
	kafka "github.com/segmentio/kafka-go"
)

// KafkaConfig holds connection parameters for a Kafka source.
type KafkaConfig struct {
	Brokers   []string
	Topic     string
	GroupID   string
	Partition int           // used only when GroupID == "" → partition-pinned reader
	BufSize   int           // channel buffer; 0 → 256
	MaxBytes  int           // max bytes per fetch; 0 → 10 MiB
	MinBytes  int           // min bytes before fetch returns; 0 → 1
	MaxWait   time.Duration // max wait before returning a short fetch; 0 → 1s
}

// KafkaOption configures a Kafka source.
type KafkaOption func(*Kafka)

// WithDLQ routes malformed messages to q instead of silently dropping them.
func WithDLQ(q *dlq.Queue) KafkaOption {
	return func(k *Kafka) { k.dlq = q }
}

// Kafka consumes a topic and turns messages into a Record stream.
// Records are JSON-encoded core.Record values. Malformed messages are sent
// to the DLQ (if configured) and skipped. The source runs until ctx is
// cancelled. Delivery guarantee: at-least-once.
type Kafka struct {
	cfg KafkaConfig
	dlq *dlq.Queue // optional; nil → silent skip
}

// NewKafka creates a Kafka source with the given config.
func NewKafka(cfg KafkaConfig, opts ...KafkaOption) *Kafka {
	k := &Kafka{cfg: cfg}
	for _, opt := range opts {
		opt(k)
	}
	return k
}

// KafkaPartitions reads the given partitions of one topic in parallel: one
// partition-pinned reader per partition, fanned into a single stream via
// NewParallel. This lifts the single-reader ingestion ceiling within one process.
// GroupID on cfg is ignored (partition readers can't be in a group).
func KafkaPartitions(cfg KafkaConfig, partitions []int, opts ...KafkaOption) core.Source {
	subs := make([]core.Source, len(partitions))
	for i, part := range partitions {
		c := cfg
		c.GroupID = ""
		c.Partition = part
		subs[i] = NewKafka(c, opts...)
	}
	return NewParallel(subs...)
}

func (k *Kafka) Read(ctx context.Context) (<-chan core.Record, error) {
	if len(k.cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka source: at least one broker required")
	}

	buf := k.cfg.BufSize
	if buf == 0 {
		buf = 256
	}
	maxBytes := k.cfg.MaxBytes
	if maxBytes == 0 {
		maxBytes = 10 * 1024 * 1024
	}
	minBytes := k.cfg.MinBytes
	if minBytes == 0 {
		minBytes = 1
	}
	maxWait := k.cfg.MaxWait
	if maxWait == 0 {
		maxWait = time.Second
	}

	rc := kafka.ReaderConfig{
		Brokers:  k.cfg.Brokers,
		Topic:    k.cfg.Topic,
		MinBytes: minBytes,
		MaxBytes: maxBytes,
		MaxWait:  maxWait,
	}
	// kafka-go forbids setting both GroupID and Partition. Group mode auto-assigns
	// partitions; partition mode pins one partition (used by KafkaPartitions to
	// read N partitions of a topic in parallel within one process).
	if k.cfg.GroupID != "" {
		rc.GroupID = k.cfg.GroupID
	} else {
		rc.Partition = k.cfg.Partition
	}
	r := kafka.NewReader(rc)

	ch := make(chan core.Record, buf)
	go func() {
		defer close(ch)
		defer r.Close() //nolint:errcheck
		for {
			msg, err := r.ReadMessage(ctx)
			if err != nil {
				return
			}
			var rec core.Record
			if err := json.Unmarshal(msg.Value, &rec); err != nil {
				if k.dlq != nil {
					k.dlq.Add(msg.Value, err.Error(), k.cfg.Topic)
				}
				continue
			}
			select {
			case ch <- rec:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}
