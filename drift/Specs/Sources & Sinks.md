---
component: sources-sinks
status: stable
package: pkg/source, pkg/sink
tested: true
---

# Sources & Sinks

See [[Core Abstractions#Source]] and [[Core Abstractions#Sink]] for interface contracts.

**Lifecycle rule:** the Pipeline owns channels. Sources and Sinks must not close channels they did not create.

---

## source.Memory

```go
NewMemory(records []core.Record) *Memory
```

- Emits all records then closes the channel
- Respects context cancellation
- Use in: unit tests, CLI demos

---

## sink.Memory

```go
NewMemory() *Memory
func (m *Memory) Records() []core.Record  // safe to call after Write returns
```

- Collects all records into an in-memory slice
- `Records()` returns a copy — safe to call after `Write`
- Use in: unit tests, integration tests

---

## source.HTTP

```go
NewHTTP(addr string) *HTTP
// src.Addr — actual address after Read() (useful with ":0")
```

- `POST /ingest` — JSON body `[]core.Record`
- Returns `400` on bad JSON, `405` on wrong method, `503` when shutting down
- Channel closes when `ctx` is cancelled

---

## sink.HTTP

```go
NewHTTP(url string) *HTTP
```

- POSTs each record as JSON to `url`
- Returns error on HTTP 4xx/5xx

---

## source.Kafka

```go
NewKafka(cfg KafkaConfig) *Kafka

type KafkaConfig struct {
    Brokers  []string
    Topic    string
    GroupID  string
    BufSize  int           // default 256
    MaxBytes int           // default 10 MiB
    MinBytes int           // default 1
    MaxWait  time.Duration // default 1s
}
```

- Decodes each Kafka message as `core.Record` JSON
- Malformed messages silently skipped (no dead-letter handling yet)
- **At-least-once** delivery — offsets committed after read
- Integration tests require `KAFKA_ADDR=<broker>` env var; skipped otherwise

---

## sink.Kafka

```go
NewKafka(cfg KafkaConfig) (*Kafka, error)

type KafkaConfig struct {
    Brokers []string
    Topic   string
    Async   bool   // fire-and-forget; higher throughput, no error feedback
}
```

- Writes each `core.Record` as a JSON Kafka message
- `Async=true` — higher throughput, errors are silent
- Returns error on marshal or write failure (unless `Async`)

---

## Invariants

1. A Source may connect to **one** Pipeline at a time
2. A Sink may consume from **one** channel at a time
3. Both respect context cancellation — never block after `ctx.Done()`
4. Kafka sources/sinks require external broker — always guard integration tests with `KAFKA_ADDR` check

---

## Planned (post-MVP)

- Dead-letter queue source/sink
- `source.Pulsar`, `source.Kinesis`

---

## See also

- [[Core Abstractions#Source]]
- [[Core Abstractions#Sink]]
- [[Testing#Integration tests]]
