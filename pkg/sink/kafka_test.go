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

// writeRetry writes msg, retrying until the topic leader is ready.
func writeRetry(t *testing.T, w *kafka.Writer, msg kafka.Message) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if err := w.WriteMessages(context.Background(), msg); err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("write timed out — topic leader not ready within deadline")
}

func TestKafkaSink_WritesRecords(t *testing.T) {
	addr := kafkaAddr(t)
	topic := "drift-test-sink-" + t.Name()

	// Prime the topic with a probe write so the leader is ready before the sink runs.
	probe := &kafka.Writer{Addr: kafka.TCP(addr), Topic: topic, AllowAutoTopicCreation: true}
	writeRetry(t, probe, kafka.Message{Value: []byte(`{}`)})
	probe.Close() //nolint:errcheck

	snk, err := NewKafka(KafkaConfig{
		Brokers: []string{addr},
		Topic:   topic,
	})
	require.NoError(t, err)

	ch := make(chan core.Record, 2)
	ch <- core.Record{Payload: map[string]any{"x": float64(10)}}
	ch <- core.Record{Payload: map[string]any{"x": float64(20)}}
	close(ch)

	require.NoError(t, snk.Write(context.Background(), ch))

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{addr},
		Topic:    topic,
		GroupID:  "drift-verify-" + t.Name(),
		MinBytes: 1,
		MaxBytes: 1e6,
		MaxWait:  2 * time.Second,
	})
	defer r.Close() //nolint:errcheck

	readCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Collect records that have the "x" field — skips the probe message.
	var got []core.Record
	for len(got) < 2 {
		msg, err := r.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("read back: %v (got %d/2)", err, len(got))
		}
		var rec core.Record
		if json.Unmarshal(msg.Value, &rec) != nil || rec.Payload["x"] == nil {
			continue
		}
		got = append(got, rec)
	}

	assert.Equal(t, float64(10), got[0].Payload["x"])
	assert.Equal(t, float64(20), got[1].Payload["x"])
}

func TestKafkaSink_NoBrokers(t *testing.T) {
	_, err := NewKafka(KafkaConfig{Topic: "x"})
	assert.ErrorContains(t, err, "broker")
}
