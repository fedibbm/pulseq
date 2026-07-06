# PulseQ

A lightweight message broker for Java/Spring Boot teams, positioned as a developer-experience-first alternative between Kafka (too heavy) and Redis Streams (too limited).

## Architecture

```
Producers → REST API → Broker Core → WebSocket → Consumers
                              ↓
                         PostgreSQL
```

- **Broker Core** — Plain Java (no framework). Custom concurrent queue using `ReentrantLock` + `Condition` variables.
- **API Layer** — Spring Boot REST + WebSocket (thin routing, no business logic)
- **Persistence** — PostgreSQL, write-before-acknowledge pattern
- **Client SDK** — Plain Java library with embedded synchronous test mode for JUnit
- **Dashboard** — Angular, real-time queue depth/throughput/DLQ view

## Core Features

- **At-least-once delivery** with visibility timeouts
- **ACK/NACK** with REJECTED (poison pill) and FAILED (transient) distinction
- **Dead-letter queue** after configurable max retries
- **Backpressure** — blocking enqueue when queue capacity is reached
- **Concurrent queue** — `ReentrantLock` + `Condition` variables (not `synchronized`)
- **PriorityQueue-based timeout checker** — lazy cleanup of expired in-flight messages
- **Per-topic consumer threads** with automatic lifecycle management

## Core Components

| Component | Responsibility |
|---|---|
| `Message` | Message data model with status, delivery tracking, visibility timeout |
| `MessageQueue` | Concurrent blocking queue with enqueue/dequeue/ack/nack, in-flight tracking, DLQ |
| `DeadLetterQueue` | Stores messages that exceeded max retries or were explicitly rejected |
| `QueueManager` | Topic registry — creates and routes to per-topic queues |
| `Dispatcher` | Manages per-topic consumer threads, pushes messages to subscribers |
| `MessageListener` | Interface for receiving dispatched messages (framework-agnostic) |
| `VisibilityTimeoutChecker` | Scheduled checker that requeues timed-out in-flight messages |

## Message Flow

1. Producer publishes to a topic → `QueueManager` auto-creates queue if needed → `enqueue()`
2. Consumer subscribes via WebSocket → `Dispatcher` starts a per-topic consumer thread → `dequeue()` blocks until message available
3. Consumer processes and sends ACK → message removed from in-flight
4. On failure → NACK with `FAILED` → message retried until `maxRetries` reached → dead-lettered
5. On poison pill → NACK with `REJECTED` → immediately dead-lettered
6. Consumer crash (no ACK within 30s) → `VisibilityTimeoutChecker` requeues the message

## Building

```bash
javac *.java
```

## License

MIT
