package com.pulseq.core;

/**
 * Immutable broker tuning parameters shared by every queue.
 *
 * <p>Values can be supplied by the embedding application (e.g. from {@code application.yml}
 * in the server module) or left at their defaults.</p>
 */
public class BrokerConfig {

    /** Maximum number of messages buffered per topic queue. */
    private final int capacity;

    /** How long a delivered message stays in-flight before it is re-delivered. */
    private final long visibilityTimeoutMillis;

    /** Base delay for exponential retry backoff (2^n * base). */
    private final long retryBaseDelayMillis;

    /** Upper bound for retry backoff delays. */
    private final long maxRetryDelayMillis;

    /** Default number of delivery attempts before a message is dead-lettered. */
    private final int maxRetries;

    /** Number of dispatcher worker threads used for fan-out delivery. */
    private final int consumerThreads;

    /** Interval between visibility timeout sweeps. */
    private final long timeoutCheckRateMillis;

    public BrokerConfig(int capacity, long visibilityTimeoutMillis, long retryBaseDelayMillis,
                        long maxRetryDelayMillis, int maxRetries, int consumerThreads,
                        long timeoutCheckRateMillis) {
        this.capacity = capacity;
        this.visibilityTimeoutMillis = visibilityTimeoutMillis;
        this.retryBaseDelayMillis = retryBaseDelayMillis;
        this.maxRetryDelayMillis = maxRetryDelayMillis;
        this.maxRetries = maxRetries;
        this.consumerThreads = consumerThreads;
        this.timeoutCheckRateMillis = timeoutCheckRateMillis;
    }

    public static BrokerConfig defaults() {
        return new BrokerConfig(1000, 30_000, 500, 60_000, 3, 8, 1_000);
    }

    public int getCapacity() { return capacity; }
    public long getVisibilityTimeoutMillis() { return visibilityTimeoutMillis; }
    public long getRetryBaseDelayMillis() { return retryBaseDelayMillis; }
    public long getMaxRetryDelayMillis() { return maxRetryDelayMillis; }
    public int getMaxRetries() { return maxRetries; }
    public int getConsumerThreads() { return consumerThreads; }
    public long getTimeoutCheckRateMillis() { return timeoutCheckRateMillis; }

    @Override
    public String toString() {
        return "BrokerConfig{capacity=" + capacity + ", visibilityTimeoutMillis=" + visibilityTimeoutMillis
                + ", retryBaseDelayMillis=" + retryBaseDelayMillis + ", maxRetryDelayMillis=" + maxRetryDelayMillis
                + ", maxRetries=" + maxRetries + ", consumerThreads=" + consumerThreads
                + ", timeoutCheckRateMillis=" + timeoutCheckRateMillis + "}";
    }
}
