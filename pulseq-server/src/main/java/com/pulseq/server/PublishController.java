package com.pulseq.server;

import com.pulseq.core.Message;
import com.pulseq.core.QueueManager;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

/**
 * Thin REST API for publishing messages to a topic.
 */
@RestController
@RequestMapping("/publish")
public class PublishController {

    private final QueueManager queueManager;

    public PublishController(QueueManager queueManager) {
        this.queueManager = queueManager;
    }

    /**
     * Publishes a message. Request body: {@code {"payload": "...", "maxRetries": 3, "ttlMillis": 0}}.
     */
    @PostMapping("/{topic}")
    public ResponseEntity<Map<String, String>> publish(
            @PathVariable String topic,
            @RequestBody(required = false) PublishRequest request) {

        String normalizedTopic = topic == null ? "" : topic.trim();
        if (normalizedTopic.isEmpty()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "topic must not be empty");
        }
        if (request == null || request.payload() == null || request.payload().isBlank()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "payload must not be empty");
        }

        byte[] payload = request.payload().getBytes(StandardCharsets.UTF_8);
        String id = UUID.randomUUID().toString();
        Message message;
        if (request.ttlMillis() != null && request.ttlMillis() > 0) {
            message = new Message(id, normalizedTopic, payload, request.ttlMillis());
        } else {
            int maxRetries = request.maxRetries() != null && request.maxRetries() > 0
                    ? request.maxRetries() : 3;
            message = new Message(id, normalizedTopic, payload, maxRetries);
        }

        boolean accepted = queueManager.publish(normalizedTopic, message);
        if (!accepted) {
            throw new ApiException(HttpStatus.CONFLICT, "message id already seen (duplicate publish)");
        }
        return ResponseEntity.ok(Map.of("messageId", id));
    }

    /** Request body for {@link #publish}. */
    public record PublishRequest(String payload, Integer maxRetries, Long ttlMillis) {
    }
}
