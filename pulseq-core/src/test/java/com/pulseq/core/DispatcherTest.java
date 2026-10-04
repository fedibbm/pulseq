package com.pulseq.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class DispatcherTest extends BrokerTestSupport {

    private QueueManager queueManager;
    private Dispatcher dispatcher;

    @BeforeEach
    void setUp() {
        queueManager = new QueueManager(new InMemoryMessageStore(), fastConfig());
        dispatcher = new Dispatcher(queueManager, 4);
    }

    @AfterEach
    void tearDown() {
        dispatcher.shutdown();
    }

    @Test
    void fanOutDeliversEveryMessageToEveryListener() throws InterruptedException {
        CountDownLatch a = new CountDownLatch(3);
        CountDownLatch b = new CountDownLatch(3);
        dispatcher.subscribe("t", m -> {
            a.countDown();
            queueManager.getQueue("t").ack(m.getId());
        });
        dispatcher.subscribe("t", m -> {
            b.countDown();
            queueManager.getQueue("t").ack(m.getId());
        });

        for (int i = 0; i < 3; i++) {
            queueManager.publish("t", message("m" + i, "t"));
        }
        assertTrue(a.await(5, TimeUnit.SECONDS), "listener A should get all 3");
        assertTrue(b.await(5, TimeUnit.SECONDS), "listener B should get all 3");
    }

    @Test
    void competingConsumersInSameGroupShareMessages() throws InterruptedException {
        AtomicInteger a = new AtomicInteger();
        AtomicInteger b = new AtomicInteger();
        dispatcher.subscribe("t", "workers", m -> {
            a.incrementAndGet();
            queueManager.getQueue("t").ack(m.getId());
        });
        dispatcher.subscribe("t", "workers", m -> {
            b.incrementAndGet();
            queueManager.getQueue("t").ack(m.getId());
        });

        for (int i = 0; i < 6; i++) {
            queueManager.publish("t", message("m" + i, "t"));
        }
        waitFor(() -> a.get() + b.get() == 6, 5_000);
        assertEquals(3, a.get());
        assertEquals(3, b.get());
    }

    @Test
    void fanOutAndCompetingGroupCoexistOnSameTopic() throws InterruptedException {
        AtomicInteger fan = new AtomicInteger();
        AtomicInteger workerA = new AtomicInteger();
        AtomicInteger workerB = new AtomicInteger();

        dispatcher.subscribe("t", m -> {
            fan.incrementAndGet();
            queueManager.getQueue("t").ack(m.getId());
        });
        dispatcher.subscribe("t", "workers", m -> {
            workerA.incrementAndGet();
            queueManager.getQueue("t").ack(m.getId());
        });
        dispatcher.subscribe("t", "workers", m -> {
            workerB.incrementAndGet();
            queueManager.getQueue("t").ack(m.getId());
        });

        for (int i = 0; i < 6; i++) {
            queueManager.publish("t", message("m" + i, "t"));
        }
        waitFor(() -> workerA.get() + workerB.get() == 6, 5_000);
        assertEquals(6, fan.get(), "fan-out listener gets every message");
        assertEquals(6, workerA.get() + workerB.get(), "group shares all messages");
        assertTrue(workerA.get() > 0 && workerB.get() > 0, "both workers get a share");
    }

    /**
     * Pins the documented delivery-acknowledgement semantics: a message carries a single state
     * machine, so the first ack from any recipient completes it for every other recipient too.
     *
     * <p>This is a deliberate design property, not an accident, and the report states it as a
     * limitation: per-subscriber independent at-least-once would require a per-delivery state
     * machine, which PulseQ does not implement.</p>
     */
    @Test
    void firstAckCompletesTheMessageForAllRecipients() throws InterruptedException {
        CountDownLatch fanReceived = new CountDownLatch(1);
        CountDownLatch groupReceived = new CountDownLatch(1);
        AtomicReference<Message> fanCopy = new AtomicReference<>();

        // The fan-out subscriber deliberately does not ack; it only keeps its copy.
        dispatcher.subscribe("t", m -> {
            fanCopy.set(m);
            fanReceived.countDown();
        });
        dispatcher.subscribe("t", "workers", m -> {
            queueManager.getQueue("t").ack(m.getId());
            groupReceived.countDown();
        });

        queueManager.publish("t", message("shared", "t"));
        assertTrue(fanReceived.await(5, TimeUnit.SECONDS));
        assertTrue(groupReceived.await(5, TimeUnit.SECONDS));
        waitFor(() -> fanCopy.get().getStatus() == MessageStatus.ACKNOWLEDGED, 3_000);

        assertEquals(MessageStatus.ACKNOWLEDGED, fanCopy.get().getStatus(),
                "the group member's ack completes the message for the fan-out subscriber too");
        assertFalse(queueManager.getQueue("t").ack("shared"),
                "a second ack for the same id finds nothing in flight and is a no-op");
    }

    /**
     * A single nack from one recipient moves the shared message, so other recipients observe a
     * message that has already been dead-lettered.
     */
    @Test
    void nackFromOneRecipientAffectsEveryOtherRecipient() throws InterruptedException {
        CountDownLatch fanReceived = new CountDownLatch(1);
        AtomicReference<Message> fanCopy = new AtomicReference<>();

        dispatcher.subscribe("t", m -> {
            fanCopy.set(m);
            fanReceived.countDown();
        });
        dispatcher.subscribe("t", "workers", m ->
                queueManager.getQueue("t").nack(m.getId(), Reason.REJECTED));

        queueManager.publish("t", message("doomed", "t"));
        assertTrue(fanReceived.await(5, TimeUnit.SECONDS));
        waitFor(() -> queueManager.getQueue("t").getDeadLetterQueue().size() == 1, 3_000);

        assertEquals(MessageStatus.DEAD_LETTERED, fanCopy.get().getStatus(),
                "one subscriber's reject dead-letters the message for everyone");
        assertEquals(1, queueManager.getQueue("t").getDeadLetterQueue().size());
    }

    @Test
    void slowListenerDoesNotBlockOtherListeners() throws InterruptedException {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch block = new CountDownLatch(1);
        AtomicInteger fast = new AtomicInteger();

        dispatcher.subscribe("t", m -> {
            started.countDown();
            try {
                block.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        dispatcher.subscribe("t", m -> {
            fast.incrementAndGet();
            queueManager.getQueue("t").ack(m.getId());
        });

        queueManager.publish("t", message("1", "t"));
        queueManager.publish("t", message("2", "t"));
        assertTrue(started.await(2, TimeUnit.SECONDS), "slow listener started");
        waitFor(() -> fast.get() == 2, 3_000);
        block.countDown();
    }

    @Test
    void unsubscribeStopsDelivery() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        MessageListener listener = m -> count.incrementAndGet();
        dispatcher.subscribe("t", listener);
        queueManager.publish("t", message("1", "t"));
        waitFor(() -> count.get() == 1, 2_000);

        dispatcher.onSessionClosed(listener);
        queueManager.publish("t", message("2", "t"));
        sleep(300);
        assertEquals(1, count.get(), "no delivery after unsubscribe");
    }

    @Test
    void throwingListenerDoesNotKillConsumerThread() throws InterruptedException {
        AtomicInteger good = new AtomicInteger();
        dispatcher.subscribe("t", m -> {
            throw new IllegalStateException("boom");
        });
        dispatcher.subscribe("t", m -> {
            good.incrementAndGet();
            queueManager.getQueue("t").ack(m.getId());
        });

        queueManager.publish("t", message("1", "t"));
        waitFor(() -> good.get() == 1, 3_000);
    }
}
