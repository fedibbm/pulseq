package com.pulseq.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-topic counters maintained by the broker.
 *
 * <p>All counters are thread-safe and keyed by topic name. Snapshots are taken with
 * {@link #snapshot()} which returns an immutable copy of the current values.</p>
 */
public class BrokerMetrics {

    private final Map<String, AtomicLong> queueDepth = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> published = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> acknowledged = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> deadLettered = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> retried = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> expired = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> rejected = new ConcurrentHashMap<>();

    void recordPublish(String topic) {
        queueDepth.computeIfAbsent(topic, k -> new AtomicLong()).incrementAndGet();
        published.computeIfAbsent(topic, k -> new AtomicLong()).incrementAndGet();
    }

    void recordAck(String topic) {
        queueDepth.computeIfAbsent(topic, k -> new AtomicLong()).updateAndGet(v -> Math.max(0, v - 1));
        acknowledged.computeIfAbsent(topic, k -> new AtomicLong()).incrementAndGet();
    }

    void recordDeadLetter(String topic) {
        queueDepth.computeIfAbsent(topic, k -> new AtomicLong()).updateAndGet(v -> Math.max(0, v - 1));
        deadLettered.computeIfAbsent(topic, k -> new AtomicLong()).incrementAndGet();
    }

    void recordRetry(String topic) {
        retried.computeIfAbsent(topic, k -> new AtomicLong()).incrementAndGet();
    }

    void recordExpired(String topic) {
        queueDepth.computeIfAbsent(topic, k -> new AtomicLong()).updateAndGet(v -> Math.max(0, v - 1));
        expired.computeIfAbsent(topic, k -> new AtomicLong()).incrementAndGet();
    }

    void recordRejected(String topic) {
        rejected.computeIfAbsent(topic, k -> new AtomicLong()).incrementAndGet();
    }

    public long getDepth(String topic) { return count(queueDepth, topic); }
    public long getPublished(String topic) { return count(published, topic); }
    public long getAcknowledged(String topic) { return count(acknowledged, topic); }
    public long getDeadLettered(String topic) { return count(deadLettered, topic); }
    public long getRetried(String topic) { return count(retried, topic); }
    public long getExpired(String topic) { return count(expired, topic); }
    public long getRejected(String topic) { return count(rejected, topic); }

    private static long count(Map<String, AtomicLong> map, String topic) {
        AtomicLong value = map.get(topic);
        return value != null ? value.get() : 0;
    }

    public MetricsSnapshot snapshot() {
        return new MetricsSnapshot(snapshotMap(queueDepth), snapshotMap(published), snapshotMap(acknowledged),
                snapshotMap(deadLettered), snapshotMap(retried), snapshotMap(expired), snapshotMap(rejected));
    }

    private static Map<String, Long> snapshotMap(Map<String, AtomicLong> map) {
        Map<String, Long> result = new ConcurrentHashMap<>();
        map.forEach((topic, counter) -> result.put(topic, counter.get()));
        return Map.copyOf(result);
    }
}
