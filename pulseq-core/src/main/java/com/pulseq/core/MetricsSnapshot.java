package com.pulseq.core;

import java.util.Map;

public class MetricsSnapshot {
    private final Map<String, Long> queueDepths;
    private final Map<String, Long> throughput;

    public MetricsSnapshot(Map<String, Long> queueDepths, Map<String, Long> throughput) {
        this.queueDepths = queueDepths;
        this.throughput = throughput;
    }

    public Map<String, Long> getQueueDepths() { return queueDepths; }
    public Map<String, Long> getThroughput() { return throughput; }
}
