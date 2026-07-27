package com.pulseq.core;

import java.util.ArrayList;
import java.util.List;

public interface MessageStore {
    void save(Message message);
    void markInFlight(String messageId, long visibilityExpiresAt);
    void markAcknowledged(String messageId);
    void markDeadLettered(String messageId);
    List<Message> loadAllAvailable();

    /**
     * Returns all dead-lettered messages, oldest first. Used on startup to rebuild the
     * in-memory dead-letter queues so they survive a restart.
     */
    List<Message> loadDeadLettered();

    /**
     * Deletes messages in a terminal state (acknowledged, dead-lettered, expired) whose
     * published time is before the given cutoff, so the store does not grow unbounded.
     *
     * @return number of rows removed
     */
    int sweepCompleted(long cutoffMillis);
}
