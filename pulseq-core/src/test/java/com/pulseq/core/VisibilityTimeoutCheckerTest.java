package com.pulseq.core;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Direct tests for {@link VisibilityTimeoutChecker}, the background task that re-queues
 * in-flight messages whose visibility timeout elapsed and moves retried messages whose
 * backoff elapsed back onto the available queue.
 *
 * <p>The checker is the component that makes at-least-once delivery work after a consumer
 * disappears, so it is worth exercising directly rather than only through the queue's
 * package-private {@code requeueTimedOut}.</p>
 */
class VisibilityTimeoutCheckerTest extends BrokerTestSupport {

    @Test
    void requeuesInFlightMessageWhenVisibilityTimeoutElapses() throws InterruptedException {
        QueueManager manager = new QueueManager(new InMemoryMessageStore(), fastConfig());
        manager.publish("t", message("m1", "t"));

        MessageQueue queue = manager.getQueue("t");
        Message delivered = queue.dequeue();
        assertNotNull(delivered);
        assertEquals(1, queue.inFlightCount(), "message must be in flight before the timeout");

        VisibilityTimeoutChecker checker = new VisibilityTimeoutChecker(manager, 20);
        checker.start();
        try {
            waitFor(() -> queue.inFlightCount() == 0, 2_000);
        } finally {
            checker.stop();
        }

        assertEquals(1, queue.size(), "the timed-out message must be available again");
        assertEquals(1, manager.getMetrics().getRetried("t"),
                "redelivery after a visibility timeout is counted as a retry");
    }

    @Test
    void doesNotRequeueBeforeTheTimeoutElapses() throws InterruptedException {
        BrokerConfig config = new BrokerConfig(10, 30_000, 50, 5_000, 3, 8, 1_000);
        QueueManager manager = new QueueManager(new InMemoryMessageStore(), config);
        manager.publish("t", message("m1", "t"));

        MessageQueue queue = manager.getQueue("t");
        assertNotNull(queue.dequeue());

        VisibilityTimeoutChecker checker = new VisibilityTimeoutChecker(manager, 10);
        checker.start();
        try {
            sleep(250);
            assertEquals(1, queue.inFlightCount(),
                    "a message must stay in flight while its visibility timeout is still running");
            assertEquals(0, queue.size());
        } finally {
            checker.stop();
        }
    }

    @Test
    void movesRetriedMessageBackOntoTheQueueWhenBackoffElapses() throws InterruptedException {
        // retryBaseDelay of 50 ms so the backoff elapses well within the test
        QueueManager manager = new QueueManager(new InMemoryMessageStore(), fastConfig());
        manager.publish("t", message("m1", "t"));

        MessageQueue queue = manager.getQueue("t");
        Message delivered = queue.dequeue();
        assertNotNull(delivered);

        assertTrue(queue.nack("m1", Reason.FAILED), "nack must be accepted for an in-flight message");
        assertEquals(0, queue.size(), "a nacked message must leave the available queue");
        assertTrue(queue.delayedCount() > 0, "a retry must be scheduled as a timer");

        VisibilityTimeoutChecker checker = new VisibilityTimeoutChecker(manager, 20);
        checker.start();
        try {
            waitFor(() -> queue.size() == 1, 2_000);
        } finally {
            checker.stop();
        }

        assertNotNull(queue.dequeue(), "the retried message must be deliverable again");
    }

    @Test
    void deadLettersMessageWhenVisibilityRetriesAreExhausted() throws InterruptedException {
        // A message carries its own retry budget, which takes precedence over the broker
        // default, so the budget of 1 is set on the message rather than the config.
        QueueManager manager = new QueueManager(new InMemoryMessageStore(), fastConfig());
        manager.publish("t", new Message("m1", "t", Payloads.toBytes("m1"), 1));

        MessageQueue queue = manager.getQueue("t");
        assertNotNull(queue.dequeue());

        VisibilityTimeoutChecker checker = new VisibilityTimeoutChecker(manager, 20);
        checker.start();
        try {
            waitFor(() -> queue.getDeadLetterQueue().size() == 1, 2_000);
        } finally {
            checker.stop();
        }

        assertEquals(0, queue.inFlightCount());
        assertEquals(0, queue.size());
    }

    @Test
    void acknowledgedMessageIsNotRequeued() throws InterruptedException {
        QueueManager manager = new QueueManager(new InMemoryMessageStore(), fastConfig());
        manager.publish("t", message("m1", "t"));

        MessageQueue queue = manager.getQueue("t");
        assertNotNull(queue.dequeue());
        assertTrue(queue.ack("m1"));

        VisibilityTimeoutChecker checker = new VisibilityTimeoutChecker(manager, 20);
        checker.start();
        try {
            sleep(250);
            assertEquals(0, queue.size(), "an acknowledged message must not come back");
            assertEquals(0, queue.inFlightCount());
            assertEquals(0, manager.getMetrics().getRetried("t"),
                    "acknowledging must cancel the visibility timer, so no retry is recorded");
        } finally {
            checker.stop();
        }
    }

    @Test
    void stopIsIdempotentAndObservable() {
        QueueManager manager = new QueueManager(new InMemoryMessageStore(), fastConfig());
        VisibilityTimeoutChecker checker = new VisibilityTimeoutChecker(manager, 50);

        assertFalse(checker.isStopped(), "a freshly constructed checker must be running");
        checker.start();
        checker.stop();
        assertTrue(checker.isStopped());
        checker.stop();
        assertTrue(checker.isStopped(), "stopping twice must be harmless");
    }

    @Test
    void sweepCostStaysFlatAsTopicsWithoutTimersAreAdded() throws InterruptedException {
        QueueManager manager = new QueueManager(new InMemoryMessageStore(), fastConfig());
        // A topic with an in-flight message, so the checker has real work to do.
        manager.publish("busy", message("m1", "busy"));
        MessageQueue busy = manager.getQueue("busy");
        assertNotNull(busy.dequeue());

        // Many idle topics. The checker skips these because delayedCount() is 0.
        List<String> idle = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            String topic = "idle-" + i;
            // Ids must be unique across every topic: the manager deduplicates by id, so
            // reusing one would silently skip queue creation for that topic.
            manager.publish(topic, message("idle-msg-" + i, topic));
            idle.add(topic);
        }

        VisibilityTimeoutChecker checker = new VisibilityTimeoutChecker(manager, 10);
        long start = System.nanoTime();
        checker.start();
        try {
            waitFor(() -> busy.inFlightCount() == 0, 2_000);
        } finally {
            checker.stop();
        }
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertTrue(elapsedMillis < 2_000,
                "sweeping 201 topics must stay well inside the timeout, took " + elapsedMillis + " ms");
        for (String topic : idle) {
            assertEquals(1, manager.getQueue(topic).size(), topic + " must be untouched");
        }
    }
}