# Changelog

## [0.3.0] - 2026-08-07

### Added
- **Durable dead-letter queues**: `MessageStore.loadDeadLettered()` with PostgreSQL and
  in-memory implementations; `QueueManager.recover()` rebuilds each topic's DLQ from the
  store on startup, so dead-lettered messages stay listable and replayable across restarts
  (postgres store). Schema gains a `(status, published_at)` index.
- **Retention sweep** (Kafka/Pulsar-style): `MessageStore.sweepCompleted(cutoff)` purges
  acknowledged, dead-lettered and expired messages older than the window; a new
  `RetentionSweeper` daemon runs it periodically, wired in the server and configured via
  `pulseq.retention-hours` (default 24, `0` disables) and `pulseq.retention-sweep-rate-ms`.

### Fixed
- Topics that only held dead-lettered messages no longer vanish from `/metrics` after a
  restart — recovery recreates their queue with a populated DLQ.

## [0.2.0] - 2026-08-06

### Added
- Angular 17 dashboard served by the server at `/`: live queue depths, publish/ack/
  dead-letter/retry/expired counters, per-topic depth + published sparklines, publish
  form, and per-topic DLQ inspection + replay. Served as static resources by the server
  (`WebConfig` + root→index forward via `DashboardController`), so there is a single
  origin and no CORS. Dev mode uses `ng serve --proxy-config proxy.conf.json`.
- Dead-letter REST endpoints: `GET /dlq/{topic}` (list with base64 payloads) and
  `POST /dlq/{topic}/replay` (`{"replayed": n}`), plus 3 new server E2E tests.
- Dockerfile now builds the dashboard (Node stage) and bakes it into the runtime image
  at `/app/dashboard` via `PULSEQ_DASHBOARD_PATH`; compose sets it automatically.

### Fixed
- `mvn spring-boot:run` no longer fails to locate the dashboard (relative static-location
  property never resolved) — replaced with an absolute-path-aware `WebConfig` resource
  handler that self-skips when the dashboard is absent.

## [0.1.0] - 2026-08-05

### Added
- Core broker: bounded concurrent per-topic queues with backpressure, visibility
  timeouts, retry with exponential backoff, message TTL, and per-topic dead-letter
  queues with replay support.
- Deduplication of repeated publishes at the manager level (10-minute window).
- Pluggable `MessageStore` with in-memory and PostgreSQL implementations; PostgreSQL
  store persists message lifecycle (available/in-flight/acked/dead-lettered) and
  recovers unacked messages on startup.
- REST publish endpoint (`POST /publish/{topic}`) and WebSocket consume endpoint with
  ACK/NACK protocol; competing-consumer groups and fan-out across groups.
- Health and metrics endpoints (published/acknowledged/deadLettered/retried/expired/
  rejected counters and per-topic queue depths).
- Client SDK with a single API for embedded (in-process) and network transports,
  plus exponential-backoff automatic reconnect.
- Example demos: `SdkDemo` (core scenarios), `NetworkDemo`, `ReconnectDemo`.
- Docker packaging (`Dockerfile`, `docker-compose.yml`) for a server + PostgreSQL stack.
- GitHub Actions CI (Maven build with a PostgreSQL service container).
- 41 automated tests (unit, server E2E, and PostgreSQL integration).

### Fixed
- Concurrent WebSocket sends no longer race: deliveries to a session are serialized,
  eliminating `TEXT_PARTIAL_WRITING` state errors under load.
