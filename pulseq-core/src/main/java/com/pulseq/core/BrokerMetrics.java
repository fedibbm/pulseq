package com.pulseq.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class BrokerMetrics {
    private final Map<String, AtomicLong> queueDepths;
    private final Map<String, AtomicLong> throughput;

    public BrokerMetrics() {
        this.queueDepths = new ConcurrentHashMap<>();
        this.throughput = new ConcurrentHashMap<>();
    }

    void recordPublish(String topic) {
        queueDepths.computeIfAbsent(topic, k -> new AtomicLong()).incrementAndGet();
        throughput.computeIfAbsent(topic, k -> new AtomicLong()).incrementAndGet();
    }

    void recordAck(String topic) {
        queueDepths.computeIfAbsent(topic, k -> new AtomicLong()).decrementAndGet();
        throughput.computeIfAbsent(topic, k -> new AtomicLong()).incrementAndGet();
    }

    long getDepth(String topic) {
        AtomicLong depth = queueDepths.get(topic);
        return depth != null ? depth.get() : 0;
    }

    long getThroughput(String topic) {
        AtomicLong count = throughput.get(topic);
        return count != null ? count.get() : 0;
    }

    MetricsSnapshot snapshot() {
        return new MetricsSnapshot(
                Map.copyOf(queueDepths.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()))),
                Map.copyOf(throughput.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get())))
        );
    }
}
