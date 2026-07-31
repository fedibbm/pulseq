package com.pulseq.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class QueueManagerTest extends BrokerTestSupport {

    @Test
    void publishAutoCreatesQueue() {
        QueueManager qm = new QueueManager(new InMemoryMessageStore());
        qm.publish("t", message("1", "t"));
        assertTrue(qm.hasQueue("t"));
        assertEquals(1, qm.getQueue("t").size());
    }

    @Test
    void duplicateMessageIdIsDropped() {
        QueueManager qm = new QueueManager(new InMemoryMessageStore());
        Message m = message("dup", "t");
        assertTrue(qm.publish("t", m));
        assertFalse(qm.publish("t", m), "duplicate id must be dropped");
        assertEquals(1, qm.getQueue("t").size());
        assertEquals(1, qm.getMetrics().getRejected("t"));
    }

    @Test
    void offerHonorsCapacity() throws InterruptedException {
        QueueManager qm = new QueueManager(new InMemoryMessageStore(),
                new BrokerConfig(2, 100, 50, 5_000, 3, 8, 1_000));
        assertTrue(qm.offer("t", message("1", "t"), 100, java.util.concurrent.TimeUnit.MILLISECONDS));
        assertTrue(qm.offer("t", message("2", "t"), 100, java.util.concurrent.TimeUnit.MILLISECONDS));
        assertFalse(qm.offer("t", message("3", "t"), 100, java.util.concurrent.TimeUnit.MILLISECONDS));
    }

    @Test
    void recoverRequeuesSurvivingMessages() {
        InMemoryMessageStore store = new InMemoryMessageStore();
        Message available = message("1", "t");
        Message dead = message("2", "t");
        store.save(available);
        store.save(dead);
        store.markDeadLettered(dead.getId());

        QueueManager qm = new QueueManager(store);
        qm.recover();

        assertTrue(qm.hasQueue("t"));
        assertEquals(1, qm.getQueue("t").size(), "only AVAILABLE/IN_FLIGHT messages are recovered");
    }

    @Test
    void recoverRebuildsDeadLetterQueues() {
        InMemoryMessageStore store = new InMemoryMessageStore();
        Message dead = message("dlq-1", "t");
        dead.incrementDeliveryAttempts();
        store.save(dead);
        store.markDeadLettered(dead.getId());

        QueueManager qm = new QueueManager(store);
        qm.recover();

        assertTrue(qm.hasQueue("t"));
        assertEquals(0, qm.getQueue("t").size(), "dead-lettered messages must not be re-queued");
        assertEquals(1, qm.getQueue("t").getDeadLetterQueue().size(), "DLQ must be rebuilt on recovery");
        Message restored = qm.getQueue("t").getDeadLetterQueue().list().get(0);
        assertEquals("dlq-1", restored.getId());
        assertEquals(1, restored.getDeliveryAttempts());
        assertEquals(1, qm.getQueue("t").replayDeadLettered(), "rebuilt DLQ must be replayable");
    }

    @Test
    void snapshotExposesPerTopicCounters() {
        QueueManager qm = new QueueManager(new InMemoryMessageStore());
        qm.publish("t", message("1", "t"));
        MetricsSnapshot snapshot = qm.snapshot();
        assertEquals(1L, snapshot.getPublished().get("t"));
        assertEquals(1L, snapshot.getQueueDepths().get("t"));
    }
}
