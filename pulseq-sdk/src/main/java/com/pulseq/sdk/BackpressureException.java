package com.pulseq.sdk;

/**
 * Thrown when a publish is refused because the topic queue stayed full for the whole backpressure
 * window (HTTP 429). The message was not accepted, so the caller may retry it later.
 */
public class BackpressureException extends RuntimeException {

    public BackpressureException(String message) {
        super(message);
    }
}