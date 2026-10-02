package com.pulseq.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
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
            metrics.recordDuplicate(topic);
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
        return tryOffer(topic, message, timeout, unit) == PublishResult.ACCEPTED;
    }

    /**
     * Bounded publish that distinguishes why a message was not accepted.
     *
     * <p>Callers that need to map the outcome onto an HTTP status (for example a REST API) need
     * to tell a duplicate id ({@code 409}) apart from a full queue ({@code 429}); the boolean
     * {@link #offer} collapses both into {@code false}.</p>
     *
     * @return {@link PublishResult#ACCEPTED}, {@link PublishResult#DUPLICATE} when the id was seen
     *         inside the dedup window, or {@link PublishResult#QUEUE_FULL} when the topic stayed full
     */
    public PublishResult tryOffer(String topic, Message message, long timeout,
                                  java.util.concurrent.TimeUnit unit) {
        if (!acceptNewId(message.getId())) {
            metrics.recordDuplicate(topic);
            return PublishResult.DUPLICATE;
        }
        MessageQueue queue = createQueue(topic);
        if (queue.offer(message, timeout, unit)) {
            return PublishResult.ACCEPTED;
        }
        seenMessageIds.remove(message.getId());
        metrics.recordBackpressure(topic);
        return PublishResult.QUEUE_FULL;
    }

    /** Outcome of a bounded publish attempt. */
    public enum PublishResult {
        ACCEPTED, DUPLICATE, QUEUE_FULL
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
     *
     * <p>Runs before any consumer thread starts, so it must never block: a topic whose
     * unfinished messages exceed its configured capacity would otherwise wait forever and the
     * application would never finish starting. Capacity overflow is reported as an
     * {@link IllegalStateException} instead.</p>
     *
     * @throws IllegalStateException when a topic holds more unfinished messages than its capacity
     */
    public void recover() {
        Map<String, Integer> perTopic = new LinkedHashMap<>();
        List<Message> survivors = store.loadAllAvailable();
        for (Message message : survivors) {
            perTopic.merge(message.getTopic(), 1, Integer::sum);
        }
        for (Map.Entry<String, Integer> entry : perTopic.entrySet()) {
            int capacity = config.getCapacity();
            if (entry.getValue() > capacity) {
                throw new IllegalStateException("topic '" + entry.getKey() + "' has "
                        + entry.getValue() + " unfinished messages but its capacity is " + capacity
                        + "; raise the capacity or drain the topic before restarting");
            }
        }
        for (Message message : survivors) {
            message.setStatus(MessageStatus.AVAILABLE);
            seenMessageIds.putIfAbsent(message.getId(), System.currentTimeMillis());
            createQueue(message.getTopic()).restore(message);
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
