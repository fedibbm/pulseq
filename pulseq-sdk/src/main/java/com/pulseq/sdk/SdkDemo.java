package com.pulseq.sdk;

import com.pulseq.core.*;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Interactive demo of the embedded broker, exercising fan-out, competing consumers,
 * retry-with-backoff, visibility timeouts, dead-lettering, replay and TTL expiry.
 */
public class SdkDemo {

    public static void main(String[] args) throws InterruptedException {
        MessageStore store = new InMemoryMessageStore();
        BrokerConfig config = new BrokerConfig(
                100,          // capacity
                300,          // visibility timeout ms
                100,          // retry base delay ms
                5_000,        // max retry delay ms
                3,            // max retries
                8,            // consumer threads
                100);         // timeout check rate ms
        QueueManager queueManager = new QueueManager(store, config);
        Dispatcher dispatcher = new Dispatcher(queueManager);
        VisibilityTimeoutChecker checker = new VisibilityTimeoutChecker(queueManager, config.getTimeoutCheckRateMillis());
        checker.start();

        PulseQClient client = PulseQClient.connect(queueManager, dispatcher);

        System.out.println("=== PulseQ Broker Demo ===\n");

        // 1. Fan-out: every subscriber receives every message
        AtomicInteger newsA = new AtomicInteger();
        AtomicInteger newsB = new AtomicInteger();
        client.subscribe("news", m -> { newsA.incrementAndGet(); client.ack(m.getId(), "news"); });
        client.subscribe("news", m -> { newsB.incrementAndGet(); client.ack(m.getId(), "news"); });
        for (int i = 1; i <= 3; i++) client.publish("news", "headline-" + i);
        Thread.sleep(500);
        System.out.println("[1] Fan-out    : A=" + newsA.get() + " B=" + newsB.get() + " (each should be 3)");

        // 2. Competing consumers: one group shares the messages round-robin
        AtomicInteger workerA = new AtomicInteger();
        AtomicInteger workerB = new AtomicInteger();
        client.subscribe("jobs", "workers", m -> { workerA.incrementAndGet(); client.ack(m.getId(), "jobs"); });
        client.subscribe("jobs", "workers", m -> { workerB.incrementAndGet(); client.ack(m.getId(), "jobs"); });
        for (int i = 1; i <= 6; i++) client.publish("jobs", "job-" + i);
        Thread.sleep(500);
        System.out.println("[2] Competing  : A=" + workerA.get() + " B=" + workerB.get() + " (sum should be 6)");

        // 3. Visibility timeout: a consumer that never acks -> redelivered until DLQ
        AtomicInteger timeoutDeliveries = new AtomicInteger();
        client.subscribe("timeout", m -> timeoutDeliveries.incrementAndGet());
        client.publish("timeout", "never acked");
        Thread.sleep(1_500);
        System.out.println("[3] Timeouts   : deliveries=" + timeoutDeliveries.get()
                + " (should be 3), DLQ=" + queueManager.getQueue("timeout").getDeadLetterQueue().size() + " (should be 1)");

        // 4. Reject -> DLQ -> replay -> delivered
        AtomicInteger replayDeliveries = new AtomicInteger();
        AtomicBoolean reject = new AtomicBoolean(true);
        client.subscribe("replay", m -> {
            if (reject.get()) {
                client.nack(m.getId(), "replay", Reason.REJECTED);
            } else {
                replayDeliveries.incrementAndGet();
                client.ack(m.getId(), "replay");
            }
        });
        client.publish("replay", "will be rejected then replayed");
        Thread.sleep(300);
        int dlqBefore = queueManager.getQueue("replay").getDeadLetterQueue().size();
        reject.set(false);
        int replayed = queueManager.getQueue("replay").replayDeadLettered();
        Thread.sleep(400);
        System.out.println("[4] Replay     : dlq-before=" + dlqBefore + " replayed=" + replayed
                + " delivered-after=" + replayDeliveries.get() + " dlq-after="
                + queueManager.getQueue("replay").getDeadLetterQueue().size() + " (should be 1 / 1 / 1 / 0)");

        // 5. TTL: short-lived messages are dropped if never consumed
        String ttlId = UUID.randomUUID().toString();
        queueManager.publish("ttl", new Message(ttlId, "ttl", Payloads.toBytes("expire me"), 200L));
        Thread.sleep(1_000);
        System.out.println("[5] TTL expiry : queue-size=" + queueManager.getQueue("ttl").size()
                + " expired=" + queueManager.getMetrics().getExpired("ttl") + " (should be 0 / 1)");

        checker.stop();
        dispatcher.shutdown();
        client.close();

        MetricsSnapshot snap = queueManager.snapshot();
        System.out.println("\nMetrics: published=" + snap.getPublished()
                + " acked=" + snap.getAcknowledged()
                + " deadLettered=" + snap.getDeadLettered()
                + " retried=" + snap.getRetried()
                + " expired=" + snap.getExpired());
        System.out.println("\n=== Demo Complete ===");
    }
}
