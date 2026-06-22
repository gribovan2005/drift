package vector_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/gribovan2005/drift/pkg/vector"
	kafka "github.com/segmentio/kafka-go"
)

// TestKafkaColumnarSource_ReadsBatch is a KAFKA_ADDR-guarded integration test: it
// produces a binary-columnar frame and reads it back through KafkaColumnarSource,
// confirming the decode-in-read-path source decodes a chunk correctly.
func TestKafkaColumnarSource_ReadsBatch(t *testing.T) {
	addr := os.Getenv("KAFKA_ADDR")
	if addr == "" {
		t.Skip("KAFKA_ADDR not set — skipping Kafka integration test (set KAFKA_ADDR=localhost:9092)")
	}
	topic := "drift-vec-test-" + t.Name()

	batch := vector.GenInt64("v", 1, 8, func(i int) int64 { return int64(i * 10) })[0]
	frame, err := vector.EncodeBatch(batch)
	if err != nil {
		t.Fatal(err)
	}

	w := &kafka.Writer{Addr: kafka.TCP(addr), Topic: topic, AllowAutoTopicCreation: true}
	// Retry first write to absorb KRaft leader/metadata lag.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if err := w.WriteMessages(context.Background(), kafka.Message{Value: frame}); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	w.Close() //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ch, err := vector.KafkaColumnarSource([]string{addr}, topic, 0).Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	select {
	case r := <-ch:
		if r.Chunk == nil {
			t.Fatal("expected a chunk-record")
		}
		col := r.Chunk.Int64("v")
		if len(col) != 8 || col[0] != 0 || col[7] != 70 {
			t.Fatalf("decoded chunk wrong: %v", col)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for a chunk")
	}
}
