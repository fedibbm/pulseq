package com.pulseq.server;

import com.pulseq.core.MetricsSnapshot;
import com.pulseq.core.QueueManager;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Observability endpoints for the broker.
 */
@RestController
public class MetricsController {

    private final QueueManager queueManager;

    public MetricsController(QueueManager queueManager) {
        this.queueManager = queueManager;
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of(
                "status", "UP",
                "topics", queueManager.listTopics(),
                "queueDepths", queueManager.snapshot().getQueueDepths());
    }

    @GetMapping("/metrics")
    public MetricsSnapshot metrics() {
        return queueManager.snapshot();
    }
}
