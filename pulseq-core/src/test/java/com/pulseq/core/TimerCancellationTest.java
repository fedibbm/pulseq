package com.pulseq.core;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that a cancelled timer cannot affect a later timer created for the same message id.
 *
 * <p>Cancellation is lazy: marking a timer cancelled does not remove it from the {@code DelayQueue},
 * so a stale entry stays there until its original deadline drains. The same id can acquire a new
 * timer in the meantime, notably when a dead-lettered message is replayed and delivered again. If a
 * cancelled timer were tracked by message id alone, the stale entry would be indistinguishable from
 * the new live one, and the stale deadline would act on the new delivery: the replayed message
 * would be requeued or dead-lettered before its own visibility timeout had elapsed.</p>
 */
class TimerCancellationTest extends BrokerTestSupport {

    @Test
    void replayedDeadLetterIsNotActedOnByItsOwnStaleTimer() throws InterruptedException {
        // short visibility timeout so the stale entry's deadline passes inside the test
        BrokerConfig config = new BrokerConfig(10, 300, 50, 5_000, 3, 8, 1_000);
        QueueManager manager = new QueueManager(new InMemoryMessageStore(), config);
        manager.publish("t", message("m1", "t"));
        MessageQueue queue = manager.getQueue("t");

        // deliver, then reject: dead-lettered, with a cancelled timer still pending
        assertNotNull(queue.dequeue());
        assertTrue(queue.nack("m1", Reason.REJECTED));
        assertEquals(1, queue.getDeadLetterQueue().size());

        // replay and deliver again: this creates a new timer for the same id
        assertEquals(1, queue.replayDeadLettered());
        assertNotNull(queue.dequeue(), "replayed message must be deliverable");
        assertEquals(1, queue.inFlightCount(), "replayed message must be in flight");

        // let the ORIGINAL deadline pass; only the new timer should still be live
        TimeUnit.MILLISECONDS.sleep(350);
        queue.requeueTimedOut();

        assertEquals(1, queue.inFlightCount(),
                "the stale timer must not act on the replayed delivery");
        assertEquals(0, queue.size(), "the replayed message must not be requeued early");
        assertEquals(0, queue.getDeadLetterQueue().size(),
                "the replayed message must not be dead-lettered early");
    }

    @Test
    void cancellingOneTimerDoesNotCancelANewerTimerForTheSameId() throws InterruptedException {
        BrokerConfig config = new BrokerConfig(10, 200, 50, 5_000, 3, 8, 1_000);
        QueueManager manager = new QueueManager(new InMemoryMessageStore(), config);
        manager.publish("t", message("m1", "t"));
        MessageQueue queue = manager.getQueue("t");

        // first delivery is nacked as a failure, which cancels the visibility timer and schedules a
        // retry timer for the same id; the cancelled visibility entry is still in the queue
        assertNotNull(queue.dequeue());
        assertTrue(queue.nack("m1", Reason.FAILED));
        assertEquals(0, queue.size(), "a failed message is not available until its backoff elapses");

        // drive the sweep past the retry backoff: the retry timer must still fire
        for (int i = 0; i < 100 && queue.size() == 0; i++) {
            sleep(20);
            queue.requeueTimedOut();
        }

        assertEquals(1, queue.size(), "the retry timer must requeue the message");
        assertEquals(0, queue.inFlightCount());
    }
}
