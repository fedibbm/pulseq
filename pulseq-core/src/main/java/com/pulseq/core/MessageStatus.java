package com.pulseq.core;

/**
 * Lifecycle states of a {@link Message} within a {@link MessageQueue}.
 */
public enum MessageStatus {
    /** Waiting to be delivered to a consumer. */
    AVAILABLE,
    /** Delivered to a consumer, waiting for an ack before the visibility timeout elapses. */
    IN_FLIGHT,
    /** Successfully processed and acknowledged by a consumer. */
    ACKNOWLEDGED,
    /** Exhausted its retries (or was rejected) and routed to the dead-letter queue. */
    DEAD_LETTERED,
    /** Dropped because its TTL expired before it could be consumed. */
    EXPIRED
}
