// Command kafkademo is a REAL end-to-end Kafka demo: it produces binary-columnar
// batches to a multi-partition topic, then consumes all partitions in parallel,
// decodes the binary frames, and runs a vectorized pipeline — measuring true
// over-the-wire throughput (network + decode + compute), not in-memory compute.
//
// Needs a broker. With Docker:
//
//	docker run -d --name drift-kafka -p 9092:9092 \
//	  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
//	  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
//	  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
//	  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
//	  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
//	  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
//	  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
//	  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
//	  -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk confluentinc/cp-kafka:7.6.0
//
//	go run ./cmd/kafkademo            # KAFKA_ADDR defaults to localhost:9092
//
// Note: the broker shares this laptop's CPU/RAM (in production it runs on separate
// nodes), so the number here is conservative versus a real deployment.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gribovan2005/drift/pkg/core"
	"github.com/gribovan2005/drift/pkg/source"
	"github.com/gribovan2005/drift/pkg/vector"
	"github.com/gribovan2005/drift/sdk"
	kafka "github.com/segmentio/kafka-go"
)

const (
	rowsPerBatch = 4096
	nBatches     = 1220 // ~5M rows
	partitions   = 10
	topic        = "drift-vec-demo"
)

func main() {
	addr := flag.String("addr", envOr("KAFKA_ADDR", "localhost:9092"), "kafka broker")
	flag.Parse()
	brokers := []string{*addr}
	totalRows := nBatches * rowsPerBatch

	// ── Topic: recreate fresh with N partitions ──────────────────────────────
	must(recreateTopic(brokers[0], topic, partitions))
	fmt.Printf("topic %q ready (%d partitions)\n", topic, partitions)

	// ── Produce: binary-columnar frames, round-robin across partitions ───────
	batches := vector.GenInt64("v", nBatches, rowsPerBatch, func(i int) int64 { return int64(i) })
	w := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.RoundRobin{}, BatchSize: 200, RequiredAcks: 1}
	defer w.Close()
	msgs := make([]kafka.Message, 0, nBatches)
	var wireBytes int
	for _, b := range batches {
		enc, err := vector.EncodeBatch(b)
		must(err)
		wireBytes += len(enc)
		msgs = append(msgs, kafka.Message{Value: enc})
	}
	tProd := time.Now()
	for i := 0; i < len(msgs); i += 200 {
		end := min(i+200, len(msgs))
		must(w.WriteMessages(context.Background(), msgs[i:end]...))
	}
	fmt.Printf("produced %d rows in %d binary frames (%d MB) in %s\n",
		totalRows, nBatches, wireBytes>>20, time.Since(tProd).Round(time.Millisecond))

	// ── Consume: all partitions in parallel → decode → vectorized Map ────────
	// A streaming consumer doesn't know the exact end, so we stop on idle (no new
	// rows for idleStop) and measure time-to-last-row — the real throughput,
	// excluding the idle wait.
	subs := make([]core.Source, partitions)
	for p := range subs {
		subs[p] = vector.KafkaColumnarSource(brokers, topic, p)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const idleStop = 1500 * time.Millisecond
	sink := &countSink{}
	go func() { // idle watchdog
		t := time.NewTicker(150 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				last := atomic.LoadInt64(&sink.lastNanos)
				if last > 0 && time.Since(time.Unix(0, last)) > idleStop {
					cancel()
					return
				}
			}
		}
	}()

	start := time.Now()
	err := sdk.New().
		From(source.NewParallel(subs...)).
		Apply(vector.MapInt64("v", func(x int64) int64 { return x + 1 })).
		To(sink).
		Run(ctx)
	if err != nil && ctx.Err() != context.Canceled && ctx.Err() != context.DeadlineExceeded {
		fmt.Printf("pipeline: %v\n", err)
	}

	got := atomic.LoadInt64(&sink.got)
	last := atomic.LoadInt64(&sink.lastNanos)
	elapsed := time.Unix(0, last).Sub(start) // to last row, not incl. idle wait
	rate := float64(got) / elapsed.Seconds()
	fmt.Printf("\nReal Kafka end-to-end (decode + vectorized map, %d partitions in parallel)\n", partitions)
	fmt.Printf("  consumed %d / %d rows in %s  →  %.2f M rows/s over the wire\n",
		got, totalRows, elapsed.Round(time.Millisecond), rate/1e6)
	fmt.Println("  (broker shares this laptop — in prod it's on separate nodes)")
}

// countSink counts surviving rows and records the time of the last one (for the
// idle-stop watchdog and time-to-last-row throughput).
type countSink struct {
	got       int64
	lastNanos int64
}

func (s *countSink) Write(ctx context.Context, ch <-chan core.Record) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r, ok := <-ch:
			if !ok {
				return nil
			}
			if r.Chunk != nil {
				atomic.AddInt64(&s.got, int64(r.Chunk.Len))
				atomic.StoreInt64(&s.lastNanos, time.Now().UnixNano())
			}
		}
	}
}

func recreateTopic(broker, topic string, parts int) error {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctrl, err := conn.Controller()
	if err != nil {
		return err
	}
	cc, err := kafka.Dial("tcp", net.JoinHostPort(ctrl.Host, strconv.Itoa(ctrl.Port)))
	if err != nil {
		return err
	}
	defer cc.Close()
	_ = cc.DeleteTopics(topic) // ignore "unknown topic"
	time.Sleep(500 * time.Millisecond)
	return cc.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: parts, ReplicationFactor: 1})
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
