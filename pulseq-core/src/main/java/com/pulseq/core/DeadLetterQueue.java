package com.pulseq.core;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Per-topic dead-letter storage for messages that exhausted their retries or were rejected.
 *
 * <p>Dead-lettered messages can be inspected and replayed back onto their source queue.
 * Access is synchronized so producers and consumer threads may touch it concurrently.</p>
 */
public class DeadLetterQueue {
    private final String sourceTopic;
    private final LinkedList<Message> messages;

    DeadLetterQueue(String sourceTopic) {
        this.sourceTopic = sourceTopic;
        this.messages = new LinkedList<>();
    }

    synchronized void add(Message message) {
        this.messages.addLast(message);
    }

    public synchronized List<Message> list() {
        return new ArrayList<>(this.messages);
    }

    /** Returns the oldest dead-lettered message without removing it, or null if empty. */
    public synchronized Message peek() {
        return this.messages.peekFirst();
    }

    /** Removes and returns the oldest dead-lettered message, or null if empty. */
    public synchronized Message remove() {
        return this.messages.pollFirst();
    }

    public synchronized String getSourceTopic() { return this.sourceTopic; }
    public synchronized int size() { return this.messages.size(); }
}
