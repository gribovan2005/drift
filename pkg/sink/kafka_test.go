package sink

import (
	"context"
	"encoding/json"
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

func TestKafkaSink_WritesRecords(t *testing.T) {
	addr := kafkaAddr(t)
	topic := "drift-test-sink-" + t.Name()

	// Sink writer has AllowAutoTopicCreation: true — topic is created on first write.
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
