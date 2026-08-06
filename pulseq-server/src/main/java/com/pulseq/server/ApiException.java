package com.pulseq.server;

import org.springframework.http.HttpStatus;

/**
 * An API-level error carrying an HTTP status and a client-facing message.
 */
public class ApiException extends RuntimeException {

    private final HttpStatus status;

    public ApiException(HttpStatus status, String message) {
        super(message);
        this.status = status;
    }

    public HttpStatus getStatus() {
        return status;
    }
}
