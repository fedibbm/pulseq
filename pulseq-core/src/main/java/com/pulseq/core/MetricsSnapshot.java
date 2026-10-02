package com.pulseq.core;

import java.util.Map;

/**
 * Immutable point-in-time copy of all broker counters, keyed by topic.
 */
public class MetricsSnapshot {
    private final Map<String, Long> queueDepths;
    private final Map<String, Long> published;
    private final Map<String, Long> acknowledged;
    private final Map<String, Long> deadLettered;
    private final Map<String, Long> retried;
    private final Map<String, Long> expired;
    private final Map<String, Long> rejected;
    private final Map<String, Long> duplicate;
    private final Map<String, Long> backpressure;

    public MetricsSnapshot(Map<String, Long> queueDepths, Map<String, Long> published,
                           Map<String, Long> acknowledged, Map<String, Long> deadLettered,
                           Map<String, Long> retried, Map<String, Long> expired, Map<String, Long> rejected,
                           Map<String, Long> duplicate, Map<String, Long> backpressure) {
        this.queueDepths = queueDepths;
        this.published = published;
        this.acknowledged = acknowledged;
        this.deadLettered = deadLettered;
        this.retried = retried;
        this.expired = expired;
        this.rejected = rejected;
        this.duplicate = duplicate;
        this.backpressure = backpressure;
    }

    public Map<String, Long> getQueueDepths() { return queueDepths; }
    public Map<String, Long> getPublished() { return published; }
    public Map<String, Long> getAcknowledged() { return acknowledged; }
    public Map<String, Long> getDeadLettered() { return deadLettered; }
    public Map<String, Long> getRetried() { return retried; }
    public Map<String, Long> getExpired() { return expired; }
    public Map<String, Long> getRejected() { return rejected; }
    public Map<String, Long> getDuplicate() { return duplicate; }
    public Map<String, Long> getBackpressure() { return backpressure; }
}
