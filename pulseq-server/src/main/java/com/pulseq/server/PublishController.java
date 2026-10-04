package com.pulseq.server;

import com.pulseq.core.Message;
import com.pulseq.core.QueueManager;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Thin REST API for publishing messages to a topic.
 */
@RestController
@RequestMapping("/publish")
public class PublishController {


    private final QueueManager queueManager;
    private final long publishTimeoutMillis;

    public PublishController(QueueManager queueManager, BrokerConfigProperties properties) {
        this.queueManager = queueManager;
        this.publishTimeoutMillis = properties.getPublishTimeoutMillis();
    }

    /**
     * Publishes a message.
     *
     * <p>Request body: {@code {"payload": "...", "messageId": "optional-uuid",
     * "maxRetries": 3, "ttlMillis": 0}}. {@code maxRetries} and {@code ttlMillis} are independent
     * and may be combined. When {@code messageId} is omitted the broker assigns a UUID; supplying
     * it makes the publish idempotent inside the broker's dedup window, so a retry carrying the
     * same id is answered with {@code 409} instead of being stored twice.</p>
     *
     * <p>Returns {@code 429} when the topic stays full for the whole backpressure window, rather
     * than holding the request thread indefinitely.</p>
     */
    @PostMapping("/{topic}")
    public ResponseEntity<Map<String, Object>> publish(
            @PathVariable String topic,
            @RequestBody(required = false) PublishRequest request) {

        String normalizedTopic = topic == null ? "" : topic.trim();
        if (normalizedTopic.isEmpty()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "topic must not be empty");
        }
        if (request == null || request.payload() == null || request.payload().isBlank()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "payload must not be empty");
        }

        String id = request.messageId() != null && !request.messageId().isBlank()
                ? request.messageId().trim()
                : UUID.randomUUID().toString();

        byte[] payload = request.payload().getBytes(StandardCharsets.UTF_8);
        int maxRetries = request.maxRetries() != null && request.maxRetries() > 0
                ? request.maxRetries() : 0;
        long ttlMillis = request.ttlMillis() != null && request.ttlMillis() > 0
                ? request.ttlMillis() : 0;
        Message message = new Message(id, normalizedTopic, payload, maxRetries, ttlMillis);

        QueueManager.PublishResult result = queueManager.tryOffer(
                normalizedTopic, message, publishTimeoutMillis, TimeUnit.MILLISECONDS);

        if (result == QueueManager.PublishResult.DUPLICATE) {
            throw new ApiException(HttpStatus.CONFLICT,
                    "message id '" + id + "' was already published inside the dedup window");
        }
        if (result == QueueManager.PublishResult.QUEUE_FULL) {
            throw new ApiException(HttpStatus.TOO_MANY_REQUESTS,
                    "topic '" + normalizedTopic + "' is full; retry later");
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("messageId", id);
        body.put("topic", normalizedTopic);
        body.put("maxRetries", message.getMaxRetries());
        body.put("ttlMillis", message.getTtlMillis());
        return ResponseEntity.ok(body);
    }

    /** Request body for {@link #publish}. */
    public record PublishRequest(String payload, String messageId, Integer maxRetries, Long ttlMillis) {
    }
}