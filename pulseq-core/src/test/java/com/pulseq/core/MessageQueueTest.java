package com.pulseq.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MessageQueueTest extends BrokerTestSupport {

    private InMemoryMessageStore store;
    private BrokerMetrics metrics;

    @BeforeEach
    void setUp() {
        store = new InMemoryMessageStore();
        metrics = new BrokerMetrics();
    }

    private MessageQueue queue() {
        return new MessageQueue("t", fastConfig(), store, metrics);
    }

    @Test
    void enqueueDequeueIsFifoAndAckTerminates() {
        MessageQueue q = queue();
        q.enqueue(message("1", "t"));
        q.enqueue(message("2", "t"));

        Message first = q.dequeue();
        Message second = q.dequeue();
        assertEquals("1", first.getId());
        assertEquals("2", second.getId());

        assertTrue(q.ack("1"));
        assertTrue(q.ack("2"));
        assertEquals(MessageStatus.ACKNOWLEDGED, first.getStatus());
        assertEquals(MessageStatus.ACKNOWLEDGED, second.getStatus());
        assertFalse(q.ack("1"));
    }

    @Test
    void dequeueMovesMessageToInFlight() {
        MessageQueue q = queue();
        q.enqueue(message("1", "t"));
        Message m = q.dequeue();
        assertEquals(MessageStatus.IN_FLIGHT, m.getStatus());
        assertEquals(1, q.inFlightCount());
        assertTrue(m.getVisibilityExpiresAt() > 0);
    }

    @Test
    void rejectedMessageGoesToDeadLetter() {
        MessageQueue q = queue();
        q.enqueue(message("1", "t"));
        Message m = q.dequeue();
        assertTrue(q.nack("1", Reason.REJECTED));
        assertEquals(MessageStatus.DEAD_LETTERED, m.getStatus());
        assertEquals(1, q.getDeadLetterQueue().size());
        assertEquals(0, q.inFlightCount());
    }

    @Test
    void failedMessageRetriesWithBackoffThenDeadLetters() throws InterruptedException {
        MessageQueue q = queue();
        q.enqueue(message("1", "t"));

        Message m = q.dequeue();
        int deliveries = 1;
        for (int i = 0; i < 3; i++) {
            assertTrue(q.nack("1", Reason.FAILED));
            sleep(120);
            q.requeueTimedOut();
            if (q.size() == 0) break;
            m = q.dequeue();
            deliveries++;
        }
        assertEquals(3, deliveries, "maxRetries=3 means exactly 3 deliveries before DLQ");
        assertEquals(MessageStatus.DEAD_LETTERED, m.getStatus());
        assertEquals(1, q.getDeadLetterQueue().size());
    }

    @Test
    void unackedMessageIsRequeuedAfterVisibilityTimeout() throws InterruptedException {
        MessageQueue q = queue();
        q.enqueue(message("1", "t"));
        Message m = q.dequeue();
        assertEquals(0, m.getDeliveryAttempts());

        sleep(200);
        q.requeueTimedOut();

        assertEquals(1, m.getDeliveryAttempts(), "visibility timeout increments attempts");
        assertEquals(0, q.size(), "message is waiting on retry backoff, not yet available");

        sleep(150);
        q.requeueTimedOut();
        assertEquals(1, q.size(), "message is available again");

        Message again = q.dequeue();
        assertEquals("1", again.getId());
        assertTrue(q.ack("1"));
    }

    @Test
    void deadLetteredMessagesCanBeReplayed() {
        MessageQueue q = queue();
        q.enqueue(message("1", "t"));
        Message m = q.dequeue();
        q.nack("1", Reason.REJECTED);
        assertEquals(1, q.getDeadLetterQueue().size());

        int replayed = q.replayDeadLettered();
        assertEquals(1, replayed);
        assertEquals(0, q.getDeadLetterQueue().size());
        assertEquals(0, m.getDeliveryAttempts(), "attempts reset on replay");
        assertEquals(1, q.size());
    }

    @Test
    void ttlExpiredMessagesAreDropped() throws InterruptedException {
        MessageQueue q = new MessageQueue("ttl", fastConfig(), store, metrics);
        Message shortLived = new Message("x", "ttl", Payloads.toBytes("x"), 150L);
        q.enqueue(shortLived);

        sleep(300);
        int dropped = q.cleanupExpired();
        assertEquals(1, dropped);
        assertEquals(0, q.size());
        assertEquals(MessageStatus.EXPIRED, shortLived.getStatus());
    }

    @Test
    void offerReturnsFalseWhenQueueIsFull() {
        MessageQueue q = new MessageQueue("cap", new BrokerConfig(2, 100, 50, 5_000, 3, 8, 1_000), store, metrics);
        assertTrue(q.offer(message("1", "cap"), 100, TimeUnit.MILLISECONDS));
        assertTrue(q.offer(message("2", "cap"), 100, TimeUnit.MILLISECONDS));
        assertFalse(q.offer(message("3", "cap"), 100, TimeUnit.MILLISECONDS), "queue is full -> backpressure");
    }

    @Test
    void ackAndNackUnknownIdReturnFalse() {
        MessageQueue q = queue();
        assertFalse(q.ack("nope"));
        assertFalse(q.nack("nope", Reason.FAILED));
    }
}
