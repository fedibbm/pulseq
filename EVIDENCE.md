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
| Wall time for the above | 121-148 ms across repeated runs |
| Sustained publish rate | 81 000-99 000 msg/s |
| In-process publish + dequeue + ack, n = 2000 | p50 = 1-2 us, p95 = 2-7 us, p99 = 10-32 us |

These figures measure the broker engine only, in a single JVM, with the in-memory
store. They exclude network and serialisation cost.

## End-to-end HTTP and WebSocket performance

Produced by `tools/bench.py` against the running server, 16 publisher threads,
200 messages each (3 200 messages per scenario):

| Scenario | Throughput | p50 | p95 | p99 | Responses |
| --- | --- | --- | --- | --- | --- |
| sequential, no consumer | 181 msg/s | 4.63 ms | 9.43 ms | 14.09 ms | 200 x 200 |
| drained, consumer attached | 1 179 msg/s | 12.24 ms | 25.48 ms | 34.49 ms | 3 200 x 200 |
| saturated, no consumer | 11 msg/s | 2 003 ms | 2 011 ms | 2 020 ms | 1 000 x 200, 2 200 x 429 |

Interpretation:

- Throughput scales from 181 msg/s on a single publisher thread to 1 179 msg/s
  with 16, a 6.5x improvement, so the topic lock is not the serialisation
  bottleneck at this concurrency level.
- The end-to-end numbers are roughly three orders of magnitude slower than the
  in-process numbers. The gap is HTTP request handling and JSON, not the queue.
  This is the honest comparison to present: the engine is fast, the REST
  endpoint is the expensive part.
- The saturated scenario is the important one for correctness. The queue capacity
  is 1 000, exactly 1 000 publishes returned 200, and the remaining 2 200 returned
  429. No publisher blocked indefinitely: latency clusters tightly at the
  configured 2 000 ms publish timeout instead of growing without bound. This is
  the bounded backpressure behaviour that the earlier blocking implementation
  did not provide.

## Observable behaviour verified over HTTP

| Check | Result |
| --- | --- |
| `POST /publish` with `maxRetries: 5` and `ttlMillis: 1000` | 200, both values echoed in the response |
| Same `messageId` published twice | first 200, second **409**, `duplicate` counter incremented, `published` counter unchanged |
| Empty payload | 400 |
| `/metrics` after the duplicate | `published: 1`, `duplicate: 1`, `backpressure` and `rejected` tracked separately |

## At-least-once redelivery after consumer loss

During the drained scenario the benchmark consumer acknowledged 1 721 of 3 200
messages. When it disconnected, the remaining 1 479 in-flight messages stayed
in flight until the 30 s visibility timeout elapsed, at which point the timeout
sweeper re-queued them and incremented `retried`. The counters reconcile exactly:
1 721 acknowledged + 1 479 retried = 3 200 published, with no message lost.

This is the intended at-least-once behaviour and is reported as such rather than
as a defect. It also shows the practical consequence: the visibility timeout must
be longer than the worst-case processing time of a consumer, or healthy messages
are delivered more than once.

## Test suite

| Module | Tests |
| --- | --- |
| `pulseq-core` | 45 (includes `ConcurrencyStressTest` with 4, and `PostgresMessageStoreIT` with 6 against a live database) |
| `pulseq-sdk` | 7 |
| `pulseq-server` | 13 |
| Total | 65 |

Core breakdown: `ConcurrencyStressTest` 4, `PayloadsTest` 4, `RetentionSweeperTest` 2,
`QueueManagerTest` 8, `DispatcherTest` 8, `InMemoryMessageStoreTest` 2,
`PostgresMessageStoreIT` 6, `MessageQueueTest` 9, `DeadLetterQueueTest` 2.

Without a reachable PostgreSQL instance the 6 integration tests are skipped and
the core module reports 39 passing tests, for a total of 59.