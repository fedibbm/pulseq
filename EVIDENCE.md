# Measured evidence

All numbers below were produced on the development machine, not estimated. Each
section states how to reproduce it.

Environment: OpenJDK 17.0.20.1, Linux, 8 logical CPUs, 15 GB RAM.
Broker started as `java -Xmx64m -jar pulseq-server/target/pulseq-server-0.1.0.jar`
with the in-memory store, default configuration (capacity 1000 per topic,
visibility timeout 30 s, publish timeout 2 s, 8 consumer threads).

## Artifact sizes

| Artifact | Size |
| --- | --- |
| `pulseq-core-0.1.0.jar` | 40 KB |
| `pulseq-sdk-0.1.0.jar` | 24 KB |
| `pulseq-server-0.1.0.jar` (Spring Boot fat jar) | 22 MB |
| core + SDK combined, for embedding | 62 KB |

The broker engine and client SDK are the parts a consumer of the library embeds;
the 22 MB figure is the standalone Spring Boot application, dominated by the
framework, not by broker code.

Resident set size of the running server with a 64 MB heap cap was 188 MB RSS,
which includes JVM metaspace, code cache, thread stacks and direct buffers.

## Engine-level concurrency and latency

Produced by `pulseq-core/src/test/java/com/pulseq/core/ConcurrencyStressTest.java`:

| Measurement | Result |
| --- | --- |
| 12 000 messages, 4 topics, 24 publisher threads, concurrent consumers | 0 lost, 0 duplicated, 0 double-acknowledged |
| Wall time for the above | 84-148 ms across repeated runs |
| Sustained publish rate | 81 000-142 857 msg/s |
| In-process publish + dequeue + ack, n = 2000 | p50 = 1-2 us, p95 = 2-7 us, p99 = 10-32 us |

These figures measure the broker engine only, in a single JVM, with the in-memory
store. They exclude network and serialisation cost.

## Timer cancellation cost

Produced by `ConcurrencyStressTest.measureCancellationCostAsInFlightDepthGrows`.
The broker cancels a pending timer by recording the id as cancelled in O(1),
rather than calling `DelayQueue.remove`, which is a linear scan.

| In-flight depth | p50 cost to acknowledge (and cancel the timer) |
| --- | --- |
| 100 | 276-405 ns |
| 3 200 | 253-405 ns |
| Ratio for a 32x depth increase | 0.66x - 0.92x |

The cost is flat: a 32x increase in depth does not increase the per-cancel cost,
which is the observable signature of the O(1) path. The test asserts the ratio
stays below 3x. Replacing the O(1) cancellation with an O(n) `DelayQueue` scan
was measured at 5.29x on the same hardware, which fails the assertion.

## End-to-end HTTP and WebSocket performance

Produced by `tools/bench.py` against the running server, 16 publisher threads,
200 messages each (3 200 messages per scenario).

Methodology, which materially affects the numbers:

- Each drained run uses its own topic, so a previous run's residue cannot decide
  what the next run measures.
- The consumer stays attached until every accepted message has been acknowledged.
  A run that cannot drain within 120 s is reported as such and excluded from the
  throughput range rather than being counted as a fast result.
- The publish phase and the drain phase are timed separately, so a slow drain
  cannot hide inside the publish figure.

Five drained runs per store, all of which acknowledged 3 200 of 3 200:

| Store | Run 1 | Run 2 | Run 3 | Run 4 | Run 5 |
| --- | --- | --- | --- | --- | --- |
| in-memory (msg/s) | 1 107 | 1 495 | 2 152 | 2 436 | 3 235 |
| PostgreSQL (msg/s) | 396 | 571 | 608 | 639 | 639 |

| Store | Range (5 runs) | Steady state (last 3) | p50 | p95 | p99 |
| --- | --- | --- | --- | --- | --- |
| in-memory | 1 107 - 3 235 msg/s | 2 152 - 3 235 msg/s | 4.54-12.78 ms | 8.18-28.14 ms | 10.22-36.80 ms |
| PostgreSQL | 396 - 639 msg/s | 608 - 639 msg/s | 12.90-16.44 ms | 65.54-126.00 ms | 94.16-223.02 ms |

Single publisher thread, no consumer, for reference: in-memory 412 msg/s
(p50 1.95 ms), PostgreSQL 173 msg/s (p50 4.99 ms).

Interpretation:

- Both stores warm up over the first runs and then plateau, so the steady-state
  figures are the ones to quote. The first run is not representative: the in-memory
  store more than doubles by run 5 as the JIT compiles the hot path.
- The in-memory store reaches roughly 3 200 msg/s end to end against 608-639
  msg/s for PostgreSQL, a 5x difference. That cost is the per-message store
  round-trip, which is the expected result for durable persistence and the main
  reason the store is pluggable.
- The end-to-end numbers are roughly three orders of magnitude slower than the
  in-process numbers. The gap is HTTP request handling, JSON and store I/O, not
  the queue. This is the honest comparison to present: the engine is fast, the
  REST endpoint is the expensive part.
- The saturated scenario is not a throughput measurement and is excluded from the
  tables above. It exists to show correctness: with the queue capacity at 1 000,
  exactly 1 000 publishes returned 200 and the remaining 2 200 returned 429, with
  latency clustered tightly at the configured 2 000 ms publish timeout rather than
  growing without bound. That is the bounded backpressure behaviour the earlier
  blocking implementation did not provide.

## Observable behaviour verified over HTTP

| Check | Result |
| --- | --- |
| `POST /publish` with `maxRetries: 5` and `ttlMillis: 1000` | 200, both values echoed in the response |
| Same `messageId` published twice | first 200, second **409**, `duplicate` counter incremented, `published` counter unchanged |
| Empty payload | 400 |
| `/metrics` after the duplicate | `published: 1`, `duplicate: 1`, `backpressure` and `rejected` tracked separately |

## At-least-once redelivery after consumer loss

Measured separately from the throughput runs, by publishing 3 200 messages,
acknowledging 1 721 of them, and then disconnecting the consumer. The remaining
1 479 in-flight messages stayed in flight until the 30 s visibility timeout
elapsed, at which point the timeout sweeper re-queued them and incremented
`retried`. The counters reconcile exactly: 1 721 acknowledged + 1 479 retried =
3 200 published, with no message lost.

This is the intended at-least-once behaviour and is reported as such rather than
as a defect. It also shows the practical consequence: the visibility timeout must
be longer than the worst-case processing time of a consumer, or healthy messages
are delivered more than once.

It is stated as a single observation from one run, not as a measured rate. The
reconnect suite covers repeated loss; this covers the timeout path.

## Test suite

| Module | Tests |
| --- | --- |
| `pulseq-core` | 53 (includes `ConcurrencyStressTest` with 5 and `PostgresMessageStoreIT` with 6 against a live database) |
| `pulseq-sdk` | 12 |
| `pulseq-server` | 13 |
| Total | 78 |

Core breakdown: `ConcurrencyStressTest` 5, `PayloadsTest` 4, `RetentionSweeperTest` 2,
`QueueManagerTest` 8, `DispatcherTest` 8, `InMemoryMessageStoreTest` 2,
`PostgresMessageStoreIT` 6, `MessageQueueTest` 9, `VisibilityTimeoutCheckerTest` 7,
`DeadLetterQueueTest` 2.

SDK breakdown: `ReconnectPolicyTest` 4, `WebSocketTransportReconnectTest` 5,
`InProcessTransportTest` 3.

Server breakdown: `PublishControllerTest` 7, `DlqControllerTest` 3,
`WebSocketE2ETest` 2, `PublishBackpressureTest` 1.

Without a reachable PostgreSQL instance the 6 integration tests are skipped and
the core module reports 47 passing tests, for a total of 72.

## Reconnect coverage

`ReconnectPolicyTest` covers the backoff arithmetic on its own. Correct
arithmetic does not prove the transport uses it, so
`WebSocketTransportReconnectTest` drives the real `WebSocketTransport` against a
minimal hand-rolled WebSocket endpoint over a real socket, severs the connection,
and asserts the subscription resumes. Removing the `onClose` -> `scheduleReconnect`
wiring was confirmed to fail two of these tests.