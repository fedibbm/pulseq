# PulseQ

A lightweight message broker for Java/Spring Boot teams — a developer-experience-first
alternative between Kafka (too heavy) and Redis Streams (too limited).

> **Want a guided tour?** See [DEMO.md](DEMO.md) for a scripted walkthrough of every
> core behavior with expected outputs.

## Modules

| Module | Description |
|---|---|
| `pulseq-raft` | Raft consensus written from scratch: leader election, log replication, a log on disk, TCP between brokers. See its [README](pulseq-raft/README.md) |
| `pulseq-core` | Framework-free broker core: concurrent queues, dispatcher, dead-letter queues, visibility timeouts, persistence SPI, and the clustered broker |
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

## Cluster mode

Three or more PulseQ servers can run as a cluster that keeps serving when one is lost. The
brokers agree on every change through Raft (implemented from scratch in `pulseq-raft`): a
publish or an acknowledgement is confirmed only once a majority of brokers hold it on disk.
One broker is the leader and serves clients; if it stops, another takes over with every
message that was not yet acknowledged. A cluster of three survives losing one broker, a
cluster of five survives losing two.

```bash
NODES="0@127.0.0.1:9100@http://127.0.0.1:8080,1@127.0.0.1:9101@http://127.0.0.1:8081,2@127.0.0.1:9102@http://127.0.0.1:8082"
for i in 0 1 2; do
  java -jar pulseq-server/target/pulseq-server-0.1.0.jar \
    --server.port=808$i \
    --pulseq.cluster.enabled=true --pulseq.cluster.node-id=$i \
    --pulseq.cluster.nodes=$NODES --pulseq.cluster.data-dir=./data/node-$i &
done
```

| Setting | Meaning |
|---|---|
| `pulseq.cluster.enabled` | `true` to run as one broker of a cluster (default `false`) |
| `pulseq.cluster.node-id` | This broker's id |
| `pulseq.cluster.nodes` | Every broker as `id@clusterHost:clusterPort@publicUrl`, comma-separated, the same list on all |
| `pulseq.cluster.data-dir` | Where this broker keeps its Raft log |

What clients see:

- **Publishing to a broker that is not the leader** answers `307` with the leader's URL, so
  an HTTP client that follows redirects (`curl -L`) needs no cluster knowledge. During an
  election the answer is `503` with `Retry-After`.
- **`GET /health`** works on every broker and reports its role and the leader's URL.
  **`GET /health/leader`** answers `200` only on the leader, for a load balancer or a
  Kubernetes readiness probe.
- **Consumers** may connect to any broker, but only the leader delivers. A consumer that
  wants no interruption connects to all brokers.
- **Delivery stays at-least-once.** A message being processed when the leader is lost is
  delivered again by the next leader.

Current limits: the list of brokers is fixed at start, the Raft log is never compacted, the
duplicate-id window does not carry over to a new leader, and traffic between brokers is
neither authenticated nor encrypted, so the cluster port must be private. Publishes on one
topic are confirmed one at a time.

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
