package com.pulseq.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A thread-safe, bounded FIFO queue for a single topic.
 *
 * <p>Delivers at-least-once: a consumed message enters {@code IN_FLIGHT} with a visibility
 * timeout; if the consumer does not ack before it elapses the message is re-queued (and its
 * delivery attempts incremented). Failed deliveries are retried with exponential backoff.
 * Messages that exhaust their retries, or are rejected outright, are routed to the topic's
 * {@link DeadLetterQueue}.</p>
 *
 * <p>Both visibility timeouts and retry backoff are tracked through a single
 * {@link DelayQueue}, so the periodic {@link VisibilityTimeoutChecker} only wakes up queues
 * that actually have pending timers.</p>
 */
public class MessageQueue {

    private final String topic;
    private final int capacity;
    private final long visibilityTimeoutMillis;
    private final long retryBaseDelayMillis;
    private final long maxRetryDelayMillis;
    private final int defaultMaxRetries;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();
    private final LinkedList<Message> messages = new LinkedList<>();
    private final Map<String, Message> inFlight = new HashMap<>();
    private final DelayQueue<DelayedMessage> delays = new DelayQueue<>();
    private final DeadLetterQueue deadLetterQueue;
    private final MessageStore store;
    private final BrokerMetrics metrics;

    public MessageQueue(String topic, BrokerConfig config, MessageStore store, BrokerMetrics metrics) {
        this.topic = topic;
        this.capacity = config.getCapacity();
        this.visibilityTimeoutMillis = config.getVisibilityTimeoutMillis();
        this.retryBaseDelayMillis = config.getRetryBaseDelayMillis();
        this.maxRetryDelayMillis = config.getMaxRetryDelayMillis();
        this.defaultMaxRetries = config.getMaxRetries();
        this.deadLetterQueue = new DeadLetterQueue(topic);
        this.store = store;
        this.metrics = metrics;
    }

    /**
     * Appends a message, blocking while the queue is full (producer backpressure).
     */
    void enqueue(Message message) {
        lock.lock();
        try {
            while (messages.size() >= capacity) {
                notFull.await();
            }
            store.save(message);
            messages.addLast(message);
            notEmpty.signal();
            metrics.recordPublish(topic);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Attempts to append a message, waiting up to the given timeout if the queue is full.
     *
     * @return true when the message was accepted, false when the queue stayed full
     */
    public boolean offer(Message message, long timeout, TimeUnit unit) {
        lock.lock();
        try {
            long remainingNanos = unit.toNanos(timeout);
            while (messages.size() >= capacity) {
                if (remainingNanos <= 0) return false;
                remainingNanos = notFull.awaitNanos(remainingNanos);
            }
            store.save(message);
            messages.addLast(message);
            notEmpty.signal();
            metrics.recordPublish(topic);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes and returns the next available message, blocking until one arrives.
     */
    Message dequeue() {
        lock.lock();
        try {
            while (messages.isEmpty()) {
                notEmpty.await();
            }
            Message message = messages.removeFirst();
            long expiresAt = System.currentTimeMillis() + visibilityTimeoutMillis;
            store.markInFlight(message.getId(), expiresAt);
            message.setStatus(MessageStatus.IN_FLIGHT);
            message.setVisibilityExpiresAt(expiresAt);
            this.inFlight.put(message.getId(), message);
            this.delays.add(new DelayedMessage(message, expiresAt));
            notFull.signal();
            return message;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Acknowledges an in-flight message. Returns false when the message is unknown
     * (already acked, dead-lettered or never delivered).
     */
    public boolean ack(String messageId) {
        lock.lock();
        try {
            Message message = inFlight.remove(messageId);
            if (message != null) {
                removeFromDelays(messageId);
                store.markAcknowledged(messageId);
                message.setStatus(MessageStatus.ACKNOWLEDGED);
                metrics.recordAck(topic);
                notFull.signal();
            }
            return message != null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Rejects or fails an in-flight message.
     *
     * <ul>
     *   <li>{@link Reason#REJECTED}: dead-letter immediately (poison pill).</li>
     *   <li>{@link Reason#FAILED}: retry with exponential backoff until max retries are hit,
     *       then dead-letter.</li>
     * </ul>
     *
     * @return false when the message is unknown
     */
    public boolean nack(String messageId, Reason reason) {
        lock.lock();
        try {
            Message message = inFlight.get(messageId);
            if (message == null) return false;

            message.incrementDeliveryAttempts();
            if (reason == Reason.REJECTED) {
                deadLetter(message);
                metrics.recordRejected(topic);
            } else if (message.getDeliveryAttempts() >= effectiveMaxRetries(message)) {
                deadLetter(message);
            } else {
                scheduleRetry(message);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Requeues in-flight messages whose visibility timeout expired and moves messages whose
     * retry backoff elapsed back onto the available queue.
     */
    void requeueTimedOut() {
        lock.lock();
        try {
            List<DelayedMessage> due = new ArrayList<>();
            delays.drainTo(due);
            boolean requeued = false;
            for (DelayedMessage delayed : due) {
                Message message = delayed.message();
                if (message.getRetryAt() > 0) {
                    requeueAvailable(message);
                    requeued = true;
                } else if (inFlight.containsKey(message.getId())) {
                    if (message.isExpired()) {
                        expire(message);
                    } else {
                        message.incrementDeliveryAttempts();
                        if (message.getDeliveryAttempts() >= effectiveMaxRetries(message)) {
                            deadLetter(message);
                        } else {
                            scheduleRetry(message);
                        }
                    }
                }
            }
            if (requeued) notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Drops available messages whose TTL elapsed.
     *
     * @return number of messages dropped
     */
    public int cleanupExpired() {
        lock.lock();
        try {
            int dropped = 0;
            var it = messages.iterator();
            while (it.hasNext()) {
                Message message = it.next();
                if (message.isExpired()) {
                    it.remove();
                    expire(message);
                    dropped++;
                }
            }
            return dropped;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Replays all dead-lettered messages back onto this queue with their attempts reset.
     *
     * @return number of messages replayed
     */
    public int replayDeadLettered() {
        lock.lock();
        try {
            int replayed = 0;
            Message message;
            while ((message = deadLetterQueue.remove()) != null) {
                message.resetDeliveryAttempts();
                message.setStatus(MessageStatus.AVAILABLE);
                message.setRetryAt(0);
                store.save(message);
                messages.addLast(message);
                replayed++;
            }
            if (replayed > 0) notEmpty.signalAll();
            return replayed;
        } finally {
            lock.unlock();
        }
    }

    private void requeueAvailable(Message message) {
        inFlight.remove(message.getId());
        removeFromDelays(message.getId());
        message.setRetryAt(0);
        message.setStatus(MessageStatus.AVAILABLE);
        store.save(message);
        messages.addLast(message);
    }

    private void scheduleRetry(Message message) {
        removeFromDelays(message.getId());
        long delay = backoffDelay(message.getDeliveryAttempts());
        message.setRetryAt(System.currentTimeMillis() + delay);
        delays.add(new DelayedMessage(message, message.getRetryAt()));
        metrics.recordRetry(topic);
    }

    private void deadLetter(Message message) {
        inFlight.remove(message.getId());
        removeFromDelays(message.getId());
        message.setStatus(MessageStatus.DEAD_LETTERED);
        store.save(message);
        deadLetterQueue.add(message);
        metrics.recordDeadLetter(topic);
    }

    private void expire(Message message) {
        inFlight.remove(message.getId());
        removeFromDelays(message.getId());
        message.setStatus(MessageStatus.EXPIRED);
        store.save(message);
        metrics.recordExpired(topic);
    }

    private long backoffDelay(int attempts) {
        if (retryBaseDelayMillis <= 0) return 0;
        long factor;
        try {
            factor = 1L << (attempts - 1);
        } catch (ArithmeticException e) {
            factor = Long.MAX_VALUE;
        }
        long delay = retryBaseDelayMillis * factor;
        if (delay < 0) return maxRetryDelayMillis;
        return Math.min(delay, maxRetryDelayMillis);
    }

    private int effectiveMaxRetries(Message message) {
        return message.getMaxRetries() > 0 ? message.getMaxRetries() : defaultMaxRetries;
    }

    private void removeFromDelays(String messageId) {
        delays.remove(new DelayedMessage(messageId));
    }

    public DeadLetterQueue getDeadLetterQueue() { return deadLetterQueue; }

    public String getTopic() { return topic; }

    public int size() {
        lock.lock();
        try {
            return messages.size();
        } finally {
            lock.unlock();
        }
    }

    public int inFlightCount() {
        lock.lock();
        try {
            return inFlight.size();
        } finally {
            lock.unlock();
        }
    }

    /** Number of pending visibility/retry timers (cheap check used by the timeout sweeper). */
    public int delayedCount() {
        return delays.size();
    }

    /**
     * A {@link Delayed} wrapper around a {@link Message}; equality is by message id so that
     * timed-out entries can be removed when a message is acked or dead-lettered.
     */
    private static final class DelayedMessage implements Delayed {
        private final Message message;
        private final long deadlineMillis;
        private final String id;

        DelayedMessage(Message message, long deadlineMillis) {
            this.message = message;
            this.deadlineMillis = deadlineMillis;
            this.id = message.getId();
        }

        DelayedMessage(String id) {
            this.message = null;
            this.deadlineMillis = -1;
            this.id = id;
        }

        Message message() { return message; }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(deadlineMillis - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(deadlineMillis, ((DelayedMessage) other).deadlineMillis);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof DelayedMessage dm && id.equals(dm.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }
}
