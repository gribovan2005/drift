---
type: index
---

# Drift

Streaming data processing engine for Go. Single binary, zero external dependencies for the core.

**Two differentiators vs Flink:**
1. [[Schema Evolution]] — operators adapt to schema changes without pipeline restart
2. [[AI Debugger]] — Gemini explains bottlenecks in plain language; not in the data path

---

## Architecture

- [[Overview]] — abstractions, data flow, non-goals
- [[Core Abstractions]] — Record, Schema, Operator, Source, Sink, Flusher

## Specs

- [[Schema Evolution]] — live schema propagation contract
- [[Operators]] — Map, Filter, FlatMap, SchemaAdapter, TumblingWindow, SlidingWindow
- [[Sources & Sinks]] — Memory, HTTP, Kafka
- [[AI Debugger]] — metrics → Gemini → plain-language diagnosis

## Development

- [[Workflow]] — AI-driven dev: spec → code → test
- [[Conventions]] — code rules, error handling, concurrency
- [[Testing]] — levels, requirements, regression gates

---

## Module layout

| Package | Responsibility | May import |
|---|---|---|
| `pkg/core` | Interfaces only | — |
| `pkg/schema` | SchemaRegistry | core |
| `pkg/operator` | Built-in operators | core |
| `pkg/pipeline` | DAG executor | core, metrics |
| `pkg/source` | Source implementations | core |
| `pkg/sink` | Sink implementations | core |
| `pkg/metrics` | OperatorMetrics | core |
| `pkg/ai` | Gemini debugger | metrics, core |
| `pkg/web` | SSE server + embedded UI | pipeline, ai, schema |
| `cmd/demo` | Demo CLI | all |

**Import rule**: `pkg/core` must never import other `pkg/` packages.
