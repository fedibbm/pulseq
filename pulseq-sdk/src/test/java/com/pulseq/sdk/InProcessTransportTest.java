package com.pulseq.sdk;

import com.pulseq.core.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class InProcessTransportTest {

    private QueueManager queueManager;
    private Dispatcher dispatcher;
    private VisibilityTimeoutChecker checker;
    private PulseQClient client;

    @BeforeEach
    void setUp() {
        queueManager = new QueueManager(new InMemoryMessageStore());
        dispatcher = new Dispatcher(queueManager);
        checker = new VisibilityTimeoutChecker(queueManager);
        checker.start();
        client = PulseQClient.connect(queueManager, dispatcher);
    }

    @AfterEach
    void tearDown() {
        checker.stop();
        dispatcher.shutdown();
        client.close();
    }

    @Test
    void publishSubscribeAckRoundTrip() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

        client.subscribe("t", message -> {
            received.add(message.getPayloadAsString());
            client.ack(message.getId(), "t");
            latch.countDown();
        });

        String id = client.publish("t", "hello");
        assertNotNull(id);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(List.of("hello"), received);
        assertEquals(0, queueManager.getQueue("t").size(), "ack removes the message");
    }

    @Test
    void groupSubscribersShareMessages() throws InterruptedException {
        AtomicInteger a = new AtomicInteger();
        AtomicInteger b = new AtomicInteger();
        CountDownLatch all = new CountDownLatch(6);

        client.subscribe("jobs", "workers", m -> {
            a.incrementAndGet();
            client.ack(m.getId(), "jobs");
            all.countDown();
        });
        client.subscribe("jobs", "workers", m -> {
            b.incrementAndGet();
            client.ack(m.getId(), "jobs");
            all.countDown();
        });

        for (int i = 0; i < 6; i++) {
            client.publish("jobs", "job-" + i);
        }
        assertTrue(all.await(5, TimeUnit.SECONDS));
        assertEquals(6, a.get() + b.get());
        assertTrue(a.get() > 0 && b.get() > 0);
    }

    @Test
    void nackFailedEventuallyDeadLetters() throws InterruptedException {
        AtomicInteger deliveries = new AtomicInteger();

        client.subscribe("poison", m -> {
            deliveries.incrementAndGet();
            client.nack(m.getId(), "poison", Reason.FAILED);
        });

        client.publish("poison", "doomed");
        assertTrue(waitForDlq("poison", 6_000), "message should reach the DLQ");
        assertEquals(3, deliveries.get(), "maxRetries=3 deliveries before DLQ");
        assertEquals(1, queueManager.getQueue("poison").getDeadLetterQueue().size());
    }

    private boolean waitForDlq(String topic, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            MessageQueue q = queueManager.getQueue(topic);
            if (q != null && q.getDeadLetterQueue().size() >= 1) return true;
            Thread.sleep(100);
        }
        return false;
    }
}
