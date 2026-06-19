package source

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

// kafkaAddr returns the broker address or skips the test.
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

func TestKafkaSource_ReadsMessages(t *testing.T) {
	addr := kafkaAddr(t)
	topic := "drift-test-source-" + t.Name()
	ensureTopic(t, addr, topic)

	// Pre-seed the topic with two records via the low-level writer.
	w := &kafka.Writer{
		Addr:  kafka.TCP(addr),
		Topic: topic,
	}
	records := []core.Record{
		{Payload: map[string]any{"n": float64(1)}},
		{Payload: map[string]any{"n": float64(2)}},
	}
	for _, r := range records {
		body, _ := json.Marshal(r)
		require.NoError(t, w.WriteMessages(context.Background(), kafka.Message{Value: body}))
	}
	w.Close() //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	src := NewKafka(KafkaConfig{
		Brokers: []string{addr},
		Topic:   topic,
		GroupID: "drift-test-group-" + t.Name(),
	})
	ch, err := src.Read(ctx)
	require.NoError(t, err)

	var got []core.Record
	for len(got) < 2 {
		select {
		case r := <-ch:
			got = append(got, r)
		case <-ctx.Done():
			t.Fatalf("timeout: got %d/2 records", len(got))
		}
	}
	cancel()

	assert.Len(t, got, 2)
}

func TestKafkaSource_SkipsMalformedMessages(t *testing.T) {
	addr := kafkaAddr(t)
	topic := "drift-test-malformed-" + t.Name()
	ensureTopic(t, addr, topic)

	w := &kafka.Writer{
		Addr:  kafka.TCP(addr),
		Topic: topic,
	}
	goodBody, _ := json.Marshal(core.Record{Payload: map[string]any{"ok": true}})
	require.NoError(t, w.WriteMessages(context.Background(), kafka.Message{Value: []byte("not-json")}))
	require.NoError(t, w.WriteMessages(context.Background(), kafka.Message{Value: goodBody}))
	w.Close() //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	src := NewKafka(KafkaConfig{
		Brokers: []string{addr},
		Topic:   topic,
		GroupID: "drift-test-skip-" + t.Name(),
	})
	ch, err := src.Read(ctx)
	require.NoError(t, err)

	select {
	case r := <-ch:
		assert.Equal(t, true, r.Payload["ok"])
	case <-ctx.Done():
		t.Fatal("timeout waiting for good record")
	}
	cancel()
}

func TestKafkaSource_NoBrokers(t *testing.T) {
	src := NewKafka(KafkaConfig{Topic: "x"})
	_, err := src.Read(context.Background())
	assert.ErrorContains(t, err, "broker")
}
