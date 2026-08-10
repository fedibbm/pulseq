# PulseQ

A lightweight message broker for Java/Spring Boot teams — a developer-experience-first
alternative between Kafka (too heavy) and Redis Streams (too limited).

> **Want a guided tour?** See [DEMO.md](DEMO.md) for a scripted walkthrough of every
> core behavior with expected outputs.

## Modules

| Module | Description |
|---|---|
| `pulseq-core` | Framework-free broker core: concurrent queues, dispatcher, dead-letter queues, visibility timeouts, persistence SPI |
| `pulseq-server` | Spring Boot 3 thin API layer: REST publish endpoint + WebSocket consume endpoint |
| `pulseq-sdk` | Plain-Java client with an in-process mode for tests and a network mode over HTTP/WebSocket |
| `pulseq-dashboard` | Angular 17 single-page dashboard served by the server at `/` |

## Architecture

```
Producers → REST API → Broker Core → WebSocket → Consumers
                          ↓
                     PostgreSQL (optional)
        └── Angular dashboard (static, served at /)
```

- **Broker Core** — plain Java, no framework. Per-topic bounded `MessageQueue`s using
  `ReentrantLock` + `Condition`, unified `DelayQueue` for visibility timeouts and retry backoff.
- **API Layer** — Spring Boot REST + WebSocket, thin routing only.
- **Persistence** — pluggable `MessageStore`: in-memory (default) or PostgreSQL, with
  recovery of unacked messages on restart.
- **Client SDK** — one `PulseQClient` API for both embedded (JUnit-friendly) and network use,
  with automatic reconnect.

## Core Features

- **At-least-once delivery** with per-message visibility timeouts
- **ACK / NACK** with `FAILED` (retry with backoff) vs `REJECTED` (poison pill → DLQ immediately)
- **Dead-letter queues** per topic, replayable back into the main queue
- **Retry with exponential backoff** (`base * 2^(attempts-1)`, capped)
- **Message TTL** — expired messages are dropped by the broker
- **Backpressure** — bounded queues, blocking (server) and non-blocking (client) publish
- **Competing-consumer groups** — round-robin fan-out within a group, fan-out across groups
- **Duplicate suppression** — dedup window per message id at the manager level
- **Durable persistence** — optional PostgreSQL store with startup recovery
- **Retention** — completed messages are swept after a configurable window (Kafka-style)

## Persistence & retention

- `AVAILABLE` / `IN_FLIGHT` messages are **recovered** after a restart — unacknowledged
  work is never lost.
- **Acknowledged messages are never replayed** (at-least-once semantics) and
  **dead-lettered messages stay dead** — but with `store=postgres` the dead-letter queues
  are **rebuilt from the store on startup**, so dead-lettered messages remain listable and
  replayable across restarts.
- Completed messages (acknowledged, dead-lettered, expired) are purged once they are older
  than `pulseq.retention-hours` (24h by default, `0` disables). This keeps PostgreSQL from
  growing unbounded.

## Wire Protocol

### Publish (REST)

```bash
curl -X POST http://localhost:8080/publish/orders \
  -H "Content-Type: application/json" \
  -d '{"payload": "hello", "maxRetries": 3, "ttlMillis": 0}'
# => {"messageId":"f3128d63-..."}
```

### Consume (WebSocket)

`ws://localhost:8080/ws?topic=orders&group=workers` (group optional)

Inbound delivery (payload is base64):

```json
{"id": "...", "topic": "orders", "deliveryAttempts": 0, "payload": "aGVsbG8="}
```

Outbound replies:

```
ACK <messageId>
NACK <messageId> FAILED
NACK <messageId> REJECTED
```

### Metrics / Health

```bash
curl http://localhost:8080/health
curl http://localhost:8080/metrics
```

### Dead-letter queues (REST)

```bash
# List dead-lettered messages for a topic (payload is base64)
curl http://localhost:8080/dlq/orders

# Replay all dead-lettered messages for a topic back into the main queue
curl -X POST http://localhost:8080/dlq/orders/replay
# => {"replayed": 3}
```

### Dashboard

The Angular dashboard is served by the server at `http://localhost:8080/` — live queue
depths, publish/ack/dead-letter/retry/expired counters, per-topic sparklines, a publish
form, and per-topic DLQ inspection + replay. The built dashboard is baked into the Docker
image, so `docker compose up` serves it with no extra step.

## SDK Usage

```java
PulseQClient client = PulseQClient.connect("http://localhost:8080");

client.subscribe("orders", message -> {
    System.out.println(message.getPayloadAsString());
    client.ack(message.getId(), message.getTopic());
});

String id = client.publish("orders", "hello");
```

For tests, run against an embedded broker with no network:

```java
QueueManager manager = new QueueManager(new InMemoryMessageStore(), BrokerConfig.defaults());
Dispatcher dispatcher = new Dispatcher(manager, 4);
PulseQClient client = PulseQClient.connect(manager, dispatcher);
```

## Configuration

Server settings live under `pulseq.*` (see `pulseq-server/src/main/resources/application.yml`):

| Property | Default | Meaning |
|---|---|---|
| `pulseq.store` | `memory` | `memory` or `postgres` |
| `pulseq.postgres-url` / `-user` / `-password` | localhost | connection for `store=postgres` |
| `pulseq.capacity` | `1000` | per-topic queue capacity |
| `pulseq.visibility-timeout-ms` | `30000` | redelivery window before a message is requeued |
| `pulseq.retry-base-delay-ms` / `max-retry-delay-ms` | `500` / `60000` | exponential backoff bounds |
| `pulseq.max-retries` | `3` | deliveries before dead-lettering |
| `pulseq.consumer-threads` | `8` | dispatcher worker pool size |
| `pulseq.timeout-check-rate-ms` | `1000` | visibility checker tick |
| `pulseq.dashboard-path` | `../pulseq-dashboard/dist/.../browser` | where the built dashboard lives; skipped when absent |
| `pulseq.retention-hours` | `24` | purge completed messages (acked/dead-lettered/expired) older than this; `0` disables |
| `pulseq.retention-sweep-rate-ms` | `60000` | how often the retention sweep runs |

## Quickstart

```bash
# Build everything and run the tests
mvn clean verify

# Run the standalone server (in-memory store); serves the dashboard at http://localhost:8080/
mvn spring-boot:run -pl pulseq-server

# Full stack with PostgreSQL + dashboard baked into the image
mvn package && docker compose up -d --build

# Rebuild the dashboard (only needed when not using Docker)
npm --prefix pulseq-dashboard install
npm --prefix pulseq-dashboard run build

# Dashboard dev mode with live reload and API proxying to a local server
npx ng serve --proxy-config proxy.conf.json   # run from pulseq-dashboard/

# Demos
mvn exec:java -pl pulseq-sdk -Dexec.mainClass=com.pulseq.sdk.SdkDemo      # 5 core scenarios
mvn exec:java -pl pulseq-sdk -Dexec.mainClass=com.pulseq.sdk.NetworkDemo  # against a live server
mvn exec:java -pl pulseq-sdk -Dexec.mainClass=com.pulseq.sdk.ReconnectDemo # reconnect resilience
```

## Testing

- **Unit tests** cover the queue lifecycle, dispatcher fan-out/groups, dedup, DLQ replay,
  TTL, and the SDK transports.
- **Server tests** cover the REST/WebSocket contract end-to-end, including competing consumers
  across separate WebSocket sessions.
- **Integration tests** (`*IT`) exercise `PostgresMessageStore` against a real database and
  self-skip when no database is reachable — CI runs them against a Postgres service container.

## License

MIT
