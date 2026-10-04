# Changelog

All notable changes to PulseQ, tracked against the commit history. Each section
groups the commits landed on that date.

## [2026-10-04]

### Fixed
These were found by an internal review of the internship report, which checked
the report's claims against the implementation rather than taking them on
trust. Each is now covered by a regression test.

- **Bounded publish backpressure.** `POST /publish` used to block indefinitely
  once a topic was full, holding a request thread and returning no status code.
  Enqueue is now bounded by a configurable timeout (default 2 s) and a full
  queue returns `429` with a `BackpressureException` body.
- **Caller-supplied message ids.** Deduplication existed in the engine but was
  unreachable from outside the JVM, because the REST and SDK layers always
  generated a fresh id. Both now accept a `messageId`, making a retried publish
  idempotent and returning `409` for a duplicate.
- **`maxRetries` dropped when a TTL was set.** `PublishController` used an
  if/else chain, so supplying `ttlMillis` silently discarded `maxRetries`. The
  two options are now applied independently.
- **Recovery could block forever.** If the persisted unfinished backlog exceeded
  a topic's capacity, startup waited on a queue that could never drain. Recovery
  is now fail-fast and reports the offending topic and counts.
- **Recovery ordering was undefined.** The store's recovery query had no
  `ORDER BY`, so restored order depended on the storage engine. Both stores now
  order by `published_at`, with the message id as tie-breaker.
- **Dead-session redelivery was 30 s for everyone.** A dropped WebSocket now
  requeues its messages through the retry path instead of waiting out the full
  visibility timeout.
- **Retry cancellation was O(n) under the queue lock.** `DelayQueue.remove`
  scanned every pending timer while holding the lock. Cancellation is now O(1)
  via lazy invalidation, and `delayedCount()` reports live timers.
- **Connection churn under the queue lock.** The PostgreSQL store opened a new
  JDBC connection per operation while the topic lock was held. It now uses a
  HikariCP pool (size configurable, default 10) and participates in the store
  lifecycle close.

### Changed
- `duplicate`, `backpressure` and `rejected` are tracked as separate metrics, so
  a retrying producer can be distinguished from a malformed or rejected message.
- Recovery re-registers the ids of unfinished messages so a recovered message
  cannot be duplicated by a concurrent republish.

### Added
- `ConcurrencyStressTest`: concurrent publish/consume with no loss or
  duplication, competing-group isolation, concurrent NACK handling, and a
  publish/acknowledge latency benchmark.
- `tools/bench.py`: reproducible end-to-end HTTP and WebSocket throughput and
  latency measurement, including the saturated case that exercises `429`.
- `EVIDENCE.md`: the measured figures with the commands to reproduce them.
- `BackpressureException` and `DuplicatePublishException` in the SDK, so a
  blocked publish is distinguishable from a rejected one.

### Known limitations
Documented rather than fixed, and pinned by tests so the behaviour is explicit:
acknowledgement is tracked per message, not per delivery, so fan-out recipients
share a single retry budget and fate. See the report's Chapter 5.

## [2026-08-10]

### Docs
- Expand README with usage and architecture.

## [2026-08-09]

### Added
- Changelog and demonstration runbook (`DEMO.md`).

### CI / packaging
- GitHub Actions build with a PostgreSQL service container.
- Docker packaging for the server and dashboard (multi-stage image, dashboard baked in).
- Docker Compose stack with PostgreSQL.

## [2026-08-08]

### Added
- Angular 17 dashboard: scaffold, then the live metrics view (queue depths,
  publish/ack/dead-letter/retry/expired counters, per-topic sparklines).
- Server test suite (REST/WebSocket contract, competing consumers).

## [2026-08-07]

### Added
- WebSocket subscription endpoint (`/subscribe/{topic}`) with ACK/NACK protocol.
- Configurable broker properties under `pulseq.*`.
- Server serves the built dashboard from the same origin.

## [2026-08-06]

### Added
- REST publish endpoint (`POST /publish/{topic}`) with validation and error handling.
- Metrics endpoint (`GET /metrics`) exposing the broker snapshot.
- Dead-letter inspection and replay endpoints (`GET /dlq/{topic}`, `POST /dlq/{topic}/replay`).

## [2026-08-05]

### Added
- Spring Boot server module (`pulseq-server`) wiring the core into a network service.
- SDK transport and reconnect tests.

## [2026-08-04]

### Added
- WebSocket transport with automatic reconnect.
- Network and reconnect demos (`NetworkDemo`, `ReconnectDemo`).

## [2026-08-03]

### Added
- Exponential-backoff reconnect policy.
- Persistence, payload and retention tests.

## [2026-07-31]

### Tests
- Broker test support and queue manager tests.
- Queue, dispatcher and dead-letter tests.

## [2026-07-30]

### Added
- Retention and deduplication wired into the queue manager.

### Build
- Test dependencies and Surefire configuration for the core module.

## [2026-07-29]

### Added
- Dispatcher reworked with a fan-out worker pool.
- Retention sweeper for completed messages.

## [2026-07-28]

### Added
- Visibility timeout and retry backoff unified into a single `DelayQueue` timer.
- Dead-letter queues made thread-safe and replayable.

## [2026-07-27]

### Added
- Metrics counters and immutable snapshot.
- Retention sweep added to the store API.

## [2026-07-24]

### Added
- Message model extended with TTL and retry scheduling.

## [2026-07-23]

### Added
- Payload serialization helpers.

## [2026-07-22]

### Added
- Immutable broker configuration.

## [2026-07-21]

### Refactor
- Sources moved into per-module packages.

## [2026-07-20]

### Build
- Converted to a multi-module Maven layout.

## [2026-07-17]

### Added
- Runnable demo and Maven build.

## [2026-07-16]

### Added
- Client transport abstraction.
- In-process transport and `PulseQClient`.

## [2026-07-15]

### Chore
- Ignore compiled class files and output directories.

## [2026-07-14]

### Added
- Broker metrics and startup recovery.

## [2026-07-13]

### Added
- PostgreSQL message store and schema.

## [2026-07-10]

### Added
- Message store abstraction with in-memory implementation.

## [2026-07-09]

### Added
- Queue manager and dispatcher.

## [2026-07-08]

### Added
- Per-topic dead-letter queue.

## [2026-07-07]

### Added
- Core message model and blocking FIFO per-topic queue.

## [2026-07-06]

### Added
- Initial commit: project skeleton and README.
