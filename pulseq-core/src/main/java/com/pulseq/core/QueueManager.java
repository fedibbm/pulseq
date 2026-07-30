package com.pulseq.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Owns the set of {@link MessageQueue}s, one per topic. Topics are created lazily on first
 * publish or subscribe. Publishing is at-least-once; duplicate message ids seen within a
 * recent window are silently dropped (idempotent producers).
 */
public class QueueManager {

    private final Map<String, MessageQueue> queues = new ConcurrentHashMap<>();
    private final Map<String, Long> seenMessageIds = new ConcurrentHashMap<>();
    private final MessageStore store;
    private final BrokerConfig config;
    private final BrokerMetrics metrics;

    private static final long DEDUP_WINDOW_MILLIS = 600_000;
    private static final int DEDUP_PRUNE_THRESHOLD = 100_000;

    public QueueManager(MessageStore store) {
        this(store, BrokerConfig.defaults());
    }

    public QueueManager(MessageStore store, BrokerConfig config) {
        this.store = store;
        this.config = config;
        this.metrics = new BrokerMetrics();
    }

    /**
     * Publishes a message, creating the topic queue on demand.
     *
     * @return false when a duplicate message id was detected and the message was dropped
     */
    public boolean publish(String topic, Message message) {
        if (!acceptNewId(message.getId())) {
            metrics.recordRejected(topic);
            return false;
        }
        createQueue(topic).enqueue(message);
        return true;
    }

    /**
     * Non-blocking publish variant: waits up to the given timeout if the topic queue is full.
     *
     * @return false when the message was a duplicate or the queue stayed full
     */
    public boolean offer(String topic, Message message, long timeout, java.util.concurrent.TimeUnit unit) {
        if (!acceptNewId(message.getId())) {
            metrics.recordRejected(topic);
            return false;
        }
        return createQueue(topic).offer(message, timeout, unit);
    }

    private boolean acceptNewId(String messageId) {
        if (messageId == null || messageId.isEmpty()) {
            return false;
        }
        long now = System.currentTimeMillis();
        if (seenMessageIds.putIfAbsent(messageId, now) != null) {
            return false;
        }
        if (seenMessageIds.size() % DEDUP_PRUNE_THRESHOLD == 0) {
            pruneSeenIds(now);
        }
        return true;
    }

    private void pruneSeenIds(long now) {
        seenMessageIds.entrySet().removeIf(e -> now - e.getValue() > DEDUP_WINDOW_MILLIS);
    }

    /**
     * Returns the queue for a topic, creating it if necessary.
     */
    public MessageQueue createQueue(String topic) {
        return queues.computeIfAbsent(topic, t -> new MessageQueue(t, config, store, metrics));
    }

    public MessageQueue getQueue(String topic) {
        return queues.get(topic);
    }

    public boolean hasQueue(String topic) {
        return queues.containsKey(topic);
    }

    /**
     * Re-queues all messages that survived a restart (AVAILABLE or IN_FLIGHT), restoring
     * them to {@link MessageStatus#AVAILABLE}, and rebuilds each topic's dead-letter queue
     * from the dead-lettered messages that survived the restart.
     */
    public void recover() {
        for (Message message : store.loadAllAvailable()) {
            message.setStatus(MessageStatus.AVAILABLE);
            publish(message.getTopic(), message);
        }
        for (Message message : store.loadDeadLettered()) {
            createQueue(message.getTopic()).getDeadLetterQueue().add(message);
        }
    }

    public List<String> listTopics() {
        return new ArrayList<>(queues.keySet());
    }

    public BrokerMetrics getMetrics() {
        return metrics;
    }

    public MetricsSnapshot snapshot() {
        return metrics.snapshot();
    }
}
