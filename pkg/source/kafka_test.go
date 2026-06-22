package source

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
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

// writeRetry writes msg to w, retrying until success or deadline.
// AllowAutoTopicCreation on the writer handles topic creation; retrying
// handles the KRaft lag between leader election and metadata visibility.
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

func TestKafkaSource_ReadsMessages(t *testing.T) {
	addr := kafkaAddr(t)
	topic := "drift-test-source-" + t.Name()

	w := &kafka.Writer{
		Addr:                   kafka.TCP(addr),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
	}
	records := []core.Record{
		{Payload: map[string]any{"n": float64(1)}},
		{Payload: map[string]any{"n": float64(2)}},
	}
	body0, _ := json.Marshal(records[0])
	writeRetry(t, w, kafka.Message{Value: body0}) // first write creates & waits for topic
	body1, _ := json.Marshal(records[1])
	require.NoError(t, w.WriteMessages(context.Background(), kafka.Message{Value: body1}))
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

	w := &kafka.Writer{
		Addr:                   kafka.TCP(addr),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
	}
	writeRetry(t, w, kafka.Message{Value: []byte("not-json")}) // creates topic, waits for leader
	goodBody, _ := json.Marshal(core.Record{Payload: map[string]any{"ok": true}})
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
