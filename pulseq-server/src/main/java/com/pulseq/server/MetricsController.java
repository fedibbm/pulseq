package com.pulseq.server;

import com.pulseq.core.Broker;
import com.pulseq.core.MetricsSnapshot;
import com.pulseq.core.QueueManager;
import com.pulseq.raft.NotLeaderException;
import com.pulseq.raft.RaftNode;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Observability endpoints for the broker.
 */
@RestController
public class MetricsController {

    private final Broker broker;
    private final ClusterProperties cluster;

    public MetricsController(Broker broker, ClusterProperties cluster) {
        this.broker = broker;
        this.cluster = cluster;
    }

    /**
     * Answers on every broker. In a cluster it also reports this broker's role and where the
     * leader is; queue details come from the leader only.
     */
    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "UP");
        if (cluster.isEnabled()) {
            body.put("nodeId", cluster.getNodeId());
        }
        try {
            QueueManager queues = broker.queues();
            if (cluster.isEnabled()) {
                body.put("role", "LEADER");
            }
            body.put("topics", queues.listTopics());
            body.put("queueDepths", queues.snapshot().getQueueDepths());
        } catch (NotLeaderException notLeader) {
            body.put("role", "FOLLOWER");
            if (notLeader.leaderId() != RaftNode.NO_LEADER) {
                body.put("leaderId", notLeader.leaderId());
                cluster.publicUrlOf(notLeader.leaderId()).ifPresent(url -> body.put("leaderUrl", url));
            }
        }
        return body;
    }

    /**
     * {@code 200} on the broker that serves clients, {@code 503} on the others. A load balancer
     * or a Kubernetes readiness probe pointed here routes clients to the leader.
     */
    @GetMapping("/health/leader")
    public ResponseEntity<Map<String, Object>> leader() {
        boolean serving = broker.isServing();
        return ResponseEntity.status(serving ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("serving", serving));
    }

    @GetMapping("/metrics")
    public MetricsSnapshot metrics() {
        return broker.queues().snapshot();
    }
}
