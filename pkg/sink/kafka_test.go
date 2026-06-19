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

// ensureTopic creates a topic and blocks until its partition leader is ready
// to accept writes. Metadata appearing in ReadPartitions is not enough —
// in KRaft the partition leader election may lag behind the metadata update.
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

	// Poll until DialLeader succeeds — that proves the partition is writable.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		lc, err := kafka.DialLeader(ctx, "tcp", addr, topic, 0)
		cancel()
		if err == nil {
			lc.Close() //nolint:errcheck
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("partition leader for topic %q not ready within deadline", topic)
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
