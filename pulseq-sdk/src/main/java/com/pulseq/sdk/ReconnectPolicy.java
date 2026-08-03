package com.pulseq.sdk;

/**
 * Exponential-backoff policy used when reconnecting a network client to the broker.
 * Resets automatically after a successful (re)connection.
 */
public class ReconnectPolicy {

    private final long baseDelayMillis;
    private final long maxDelayMillis;
    private long attempts;

    public ReconnectPolicy(long baseDelayMillis, long maxDelayMillis) {
        if (baseDelayMillis <= 0) throw new IllegalArgumentException("baseDelayMillis must be positive");
        if (maxDelayMillis < baseDelayMillis) throw new IllegalArgumentException("maxDelayMillis must be >= baseDelayMillis");
        this.baseDelayMillis = baseDelayMillis;
        this.maxDelayMillis = maxDelayMillis;
    }

    /**
     * @return the delay before the next reconnect attempt (2^n * base, capped)
     */
    public synchronized long nextDelayMillis() {
        long delay;
        try {
            delay = baseDelayMillis * (1L << Math.min(attempts, 62));
        } catch (ArithmeticException e) {
            delay = maxDelayMillis;
        }
        attempts++;
        return Math.min(delay, maxDelayMillis);
    }

    /** Resets the backoff counter after a successful connection. */
    public synchronized void reset() {
        attempts = 0;
    }

    synchronized long getAttempts() {
        return attempts;
    }
}
