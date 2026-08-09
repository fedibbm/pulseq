# PulseQ Demo Runbook

A scripted walkthrough of the broker's core behaviors. All commands run from the
repository root. Anything in `<...>` is a placeholder.

## Prerequisites

- JDK 17, Maven 3.9+, and (for the full stack) Docker with Compose
- Node 20+ only if you rebuild the dashboard locally (the Docker image bakes it in)

Build everything and run the tests once:

```bash
mvn clean verify
```

---

## Demo 1 — Embedded broker (no server)

The SDK's in-process transport talks to the broker core directly, so this needs no
running server. It demonstrates fan-out, competing consumers, visibility timeouts,
retry → dead-lettering, DLQ replay, and TTL expiry:

```bash
mvn exec:java -pl pulseq-sdk -Dexec.mainClass=com.pulseq.sdk.SdkDemo
```

Expected output:

```
=== PulseQ Broker Demo ===

[1] Fan-out    : A=3 B=3 (each should be 3)
[2] Competing  : A=3 B=3 (sum should be 6)
[3] Timeouts   : deliveries=3 (should be 3), DLQ=1 (should be 1)
[4] Replay     : dlq-before=1 replayed=1 delivered-after=1 dlq-after=0 (should be 1 / 1 / 1 / 0)
[5] TTL expiry : queue-size=0 expired=1 (should be 0 / 1)

Metrics: published={ttl=1, news=3, timeout=1, replay=1, jobs=6} acked={news=3, replay=1, jobs=6} deadLettered={timeout=1, replay=1} retried={timeout=2} expired={ttl=1}

=== Demo Complete ===
```

---

## Demo 2 — Network mode + dashboard

Start the full stack (PostgreSQL + server + dashboard). Build the server jar first —
the Docker image copies `pulseq-server/target/pulseq-server-0.1.0.jar`, so a stale jar
means a stale image:

```bash
mvn package
docker compose up -d --build
```

Wait for health, then open the dashboard:

```bash
curl -s http://localhost:8080/health
# {"queueDepths":{},"status":"UP","topics":[]}
# then browse to http://localhost:8080/ — the PulseQ dashboard loads
```

Now run the network demo against the live server. It publishes two `orders` messages
(consumed and acked over WebSocket) and one `retry` message that fails on purpose until
it exhausts its retries and lands in the dead-letter queue:

```bash
mvn exec:java -pl pulseq-sdk -Dexec.mainClass=com.pulseq.sdk.NetworkDemo
```

Expected output (ids differ each run):

```
=== PulseQ Network Demo (http://localhost:8080) ===

[orders] first order (attempt 0)
Published: 785f8ea3-..., 6b261f03-...
[orders] second order (attempt 0)
Received 2 messages over WebSocket.
[retry] attempt 0
[retry] attempt 1
[retry] attempt 2

=== Network Demo Complete (check /metrics on the server) ===
```

Watch the dashboard: the `orders` depth + published sparklines tick, `Published: 2 /
Acked: 2 / Retried: 2` for `retry`, and the DLQ counter for `retry` hits 1.

Confirm the counters and dead-lettered message:

```bash
curl -s http://localhost:8080/metrics
# {"queueDepths":{"orders":0,"retry":0},"published":{"orders":2,"retry":1},
#  "acknowledged":{"orders":2},"deadLettered":{"retry":1},"retried":{"retry":2},
#  "expired":{},"rejected":{}}

curl -s http://localhost:8080/dlq/retry
# [{"id":"4ee0a883-...","topic":"retry","status":"DEAD_LETTERED",
#   "deliveryAttempts":3,"maxRetries":3,"publishedAt":1786136733422,
#   "payload":"d2lsbCBiZSByZXRyaWVkIHVudGlsIERMUQ=="}]   # "will be retried until DLQ"
```

Replay the dead-lettered message back into the queue:

```bash
curl -s -X POST http://localhost:8080/dlq/retry/replay
# {"replayed":1}

curl -s http://localhost:8080/dlq/retry
# []
```

---

## Demo 3 — Durability across a restart

`AVAILABLE` / `IN_FLIGHT` messages are persisted (PostgreSQL) and recovered on startup.
Publish a message with **no consumer**, restart the server, and watch it come back:

```bash
curl -s -X POST http://localhost:8080/publish/survivor \
  -H "Content-Type: application/json" \
  -d '{"payload":"survive a restart"}'
# {"messageId":"f2eceded-..."}

curl -s http://localhost:8080/metrics
# "queueDepths":{"survivor":1}

docker compose restart server
# wait for /health to return 200 again

curl -s http://localhost:8080/metrics
# "queueDepths":{"survivor":1}   <- recovered from PostgreSQL
```

Note: acknowledged and dead-lettered messages are **not** replayed (at-least-once
semantics) — only unfinished work survives a restart. Dead-lettered messages do survive
as listable/replayable entries via `GET /dlq/{topic}`.

---

## Demo 4 — Retention sweep (optional)

Completed messages (acked, dead-lettered, expired) are purged after
`pulseq.retention-hours` (default 24h). To see it quickly, run a server with a tiny
window and watch its log. From a second terminal, against a fresh server on port 8081:

```bash
PULSEQ_STORE=postgres \
PULSEQ_POSTGRES_URL=jdbc:postgresql://localhost:5432/pulseq \
PULSEQ_POSTGRES_USER=pulseq \
PULSEQ_POSTGRES_PASSWORD=pulseq \
PULSEQ_RETENTION_HOURS=0.003 \        # ~11 seconds
PULSEQ_RETENTION_SWEEP_RATE_MS=1000 \
SERVER_PORT=8081 \
mvn spring-boot:run -pl pulseq-server
```

Run a demo against it, then wait ~11s:

```bash
mvn exec:java -pl pulseq-sdk -Dexec.mainClass=com.pulseq.sdk.NetworkDemo \
  -Dexec.args="http://localhost:8081"
```

The server log reports the purge:

```
PulseQ: retention sweep removed 9 completed message(s)
```

Recoverable messages (status `AVAILABLE`/`IN_FLIGHT`) are never swept.

---

## Reset / cleanup

```bash
docker compose down          # stop; the PostgreSQL volume is kept
docker compose down -v       # stop AND delete the database (start from scratch)
```

Troubleshooting:

- **Dashboard is empty after a restart** — everything you published was acked, so
  nothing was left to recover. Publish with no consumer (Demo 3) and you'll see it.
- **Stale behavior after a code change** — `mvn package` first, then
  `docker compose up -d --build`; the image bundles whatever jar was built last.
- **`PostgresMessageStoreIT` self-skips** — those tests need a reachable PostgreSQL
  (`docker compose up -d postgres`), otherwise they skip silently.
