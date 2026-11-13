package com.pulseq.server;

import com.pulseq.core.ReplicationException;
import com.pulseq.raft.NotLeaderException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Converts exceptions thrown by controllers into structured JSON error responses.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private final ClusterProperties cluster;

    public GlobalExceptionHandler(ClusterProperties cluster) {
        this.cluster = cluster;
    }

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<Map<String, Object>> handleApi(ApiException e) {
        return body(e.getStatus(), e.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgument(IllegalArgumentException e) {
        return body(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    /**
     * This broker is not the leader. When the leader is known the client is redirected to the
     * same path there; {@code 307} keeps the method and body, so a POST is repeated as a POST.
     * Otherwise an election is under way and the client should retry shortly.
     */
    @ExceptionHandler(NotLeaderException.class)
    public ResponseEntity<Map<String, Object>> handleNotLeader(NotLeaderException e, HttpServletRequest request) {
        Optional<String> leaderUrl = e.leaderId() == cluster.getNodeId()
                ? Optional.empty() // this broker was just elected and is still taking over
                : cluster.publicUrlOf(e.leaderId());
        if (leaderUrl.isPresent()) {
            String query = request.getQueryString();
            String location = leaderUrl.get() + request.getRequestURI() + (query == null ? "" : "?" + query);
            return ResponseEntity.status(HttpStatus.TEMPORARY_REDIRECT)
                    .header(HttpHeaders.LOCATION, location)
                    .body(errorBody(HttpStatus.TEMPORARY_REDIRECT, "this broker is not the leader; go to " + location));
        }
        return retryLater("no leader is serving yet; retry shortly");
    }

    /** A majority did not confirm in time. The outcome is unknown, so the client should retry. */
    @ExceptionHandler(ReplicationException.class)
    public ResponseEntity<Map<String, Object>> handleReplication(ReplicationException e) {
        return retryLater("the cluster did not confirm the request in time; retry with the same message id");
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGeneric(Exception e) {
        return body(HttpStatus.INTERNAL_SERVER_ERROR, "internal error");
    }

    private ResponseEntity<Map<String, Object>> retryLater(String message) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "1")
                .body(errorBody(HttpStatus.SERVICE_UNAVAILABLE, message));
    }

    private ResponseEntity<Map<String, Object>> body(HttpStatus status, String message) {
        return ResponseEntity.status(status).body(errorBody(status, message));
    }

    private static Map<String, Object> errorBody(HttpStatus status, String message) {
        return Map.of(
                "timestamp", Instant.now().toString(),
                "status", status.value(),
                "error", status.getReasonPhrase(),
                "message", message == null ? "unknown error" : message);
    }
}
