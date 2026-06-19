package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/andrejgribov/drift/pkg/core"
	kafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func kafkaAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("KAFKA_ADDR")
	if addr == "" {
		t.Skip("KAFKA_ADDR not set — skipping Kafka integration test (set KAFKA_ADDR=localhost:9092)")
	}
	return addr
}

// ensureTopic creates a topic and blocks until it appears in broker metadata.
// CreateTopics returns before the broker fully applies the change; polling
// ReadPartitions on a fresh connection gives us a definitive ready signal.
func ensureTopic(t *testing.T, addr, topic string) {
	t.Helper()

	conn, err := kafka.Dial("tcp", addr)
	require.NoError(t, err)
	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	conn.Close() //nolint:errcheck
	require.NoError(t, err, fmt.Sprintf("create topic %q", topic))

	// Poll until the topic is visible in a fresh metadata fetch.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		c, err := kafka.Dial("tcp", addr)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		parts, err := c.ReadPartitions(topic)
		c.Close() //nolint:errcheck
		if err == nil && len(parts) > 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("topic %q did not become available within deadline", topic)
}

func TestKafkaSink_WritesRecords(t *testing.T) {
	addr := kafkaAddr(t)
	topic := "drift-test-sink-" + t.Name()
	ensureTopic(t, addr, topic)

	snk, err := NewKafka(KafkaConfig{
		Brokers: []string{addr},
		Topic:   topic,
	})
	require.NoError(t, err)

	ch := make(chan core.Record, 2)
	ch <- core.Record{Payload: map[string]any{"x": float64(10)}}
	ch <- core.Record{Payload: map[string]any{"x": float64(20)}}
	close(ch)

	ctx := context.Background()
	require.NoError(t, snk.Write(ctx, ch))

	// Verify messages were written by reading them back.
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{addr},
		Topic:    topic,
		GroupID:  "drift-verify-" + t.Name(),
		MinBytes: 1,
		MaxBytes: 1e6,
		MaxWait:  2 * time.Second,
	})
	defer r.Close() //nolint:errcheck

	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var got []core.Record
	for len(got) < 2 {
		msg, err := r.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("read back: %v (got %d/2)", err, len(got))
		}
		var rec core.Record
		require.NoError(t, json.Unmarshal(msg.Value, &rec))
		got = append(got, rec)
	}

	assert.Equal(t, float64(10), got[0].Payload["x"])
	assert.Equal(t, float64(20), got[1].Payload["x"])
}

func TestKafkaSink_NoBrokers(t *testing.T) {
	_, err := NewKafka(KafkaConfig{Topic: "x"})
	assert.ErrorContains(t, err, "broker")
}
