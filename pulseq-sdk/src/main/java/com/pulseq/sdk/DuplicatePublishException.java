package com.pulseq.sdk;

/**
 * Thrown when a publish carrying a caller-supplied message id is rejected because the broker has
 * already accepted that id inside its dedup window. The publish is safe to retry: the broker
 * already holds the message.
 */
public class DuplicatePublishException extends RuntimeException {

    public DuplicatePublishException(String message) {
        super(message);
    }
}