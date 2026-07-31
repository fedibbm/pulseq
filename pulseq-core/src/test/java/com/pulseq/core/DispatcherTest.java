package com.pulseq.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
