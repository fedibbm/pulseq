package com.pulseq.server;

import com.pulseq.core.DeadLetterQueue;
import com.pulseq.core.Message;
import com.pulseq.core.MessageQueue;
import com.pulseq.core.QueueManager;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Per-topic dead-letter inspection and replay endpoints for the dashboard and ops tooling.
 */
@RestController
@RequestMapping("/dlq")
public class DlqController {

    private final QueueManager queueManager;

    public DlqController(QueueManager queueManager) {
        this.queueManager = queueManager;
    }

    /**
     * Lists the messages currently dead-lettered for a topic.
     */
    @GetMapping("/{topic}")
    public List<DlqEntry> list(@PathVariable String topic) {
        MessageQueue queue = queueManager.getQueue(topic);
        if (queue == null) {
            throw new ApiException(HttpStatus.NOT_FOUND, "unknown topic '" + topic + "'");
        }
        return queue.getDeadLetterQueue().list().stream().map(DlqEntry::of).toList();
    }

    /**
     * Replays all dead-lettered messages back onto the topic's main queue (attempts reset).
     */
    @PostMapping("/{topic}/replay")
    public Map<String, Integer> replay(@PathVariable String topic) {
        MessageQueue queue = queueManager.getQueue(topic);
        if (queue == null) {
            throw new ApiException(HttpStatus.NOT_FOUND, "unknown topic '" + topic + "'");
        }
        return Map.of("replayed", queue.replayDeadLettered());
    }

    /** Payloads are base64-encoded on the wire, matching the WebSocket delivery format. */
    public record DlqEntry(String id, String topic, String status, int deliveryAttempts,
                           int maxRetries, long publishedAt, String payload) {
        static DlqEntry of(Message message) {
            return new DlqEntry(
                    message.getId(),
                    message.getTopic(),
                    message.getStatus().name(),
                    message.getDeliveryAttempts(),
                    message.getMaxRetries(),
                    message.getPublishedAt(),
                    Base64.getEncoder().encodeToString(message.getPayload()));
        }
    }
}
