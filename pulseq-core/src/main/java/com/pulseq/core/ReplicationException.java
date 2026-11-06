package com.pulseq.core;

/**
 * A store operation could not be confirmed by a majority of brokers in time. The outcome is
 * unknown: the operation may still take effect later. Callers should retry, which is safe
 * because publishes are deduplicated by message id and acknowledgements are idempotent.
 */
public class ReplicationException extends RuntimeException {

    public ReplicationException(String message, Throwable cause) {
        super(message, cause);
    }
}
