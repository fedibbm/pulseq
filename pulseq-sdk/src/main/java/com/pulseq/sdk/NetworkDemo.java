package com.pulseq.sdk;

import com.pulseq.core.Reason;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs the client against a real {@code pulseq-server} over HTTP/WebSocket.
 *
 * <p>Start the server first with {@code mvn spring-boot:run -pl pulseq-server}, then run this
 * demo (defaults to http://localhost:8080).</p>
 */
public class NetworkDemo {

    public static void main(String[] args) throws InterruptedException {
        String baseUrl = args.length > 0 ? args[0] : "http://localhost:8080";
        System.out.println("=== PulseQ Network Demo (" + baseUrl + ") ===\n");

        PulseQClient client = PulseQClient.connect(baseUrl);

        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger received = new AtomicInteger();

        client.subscribe("orders", message -> {
            received.incrementAndGet();
            System.out.println("[orders] " + message.getPayloadAsString()
                    + " (attempt " + message.getDeliveryAttempts() + ")");
            client.ack(message.getId(), "orders");
            latch.countDown();
        });

        String id1 = client.publish("orders", "first order");
        String id2 = client.publish("orders", "second order");
        System.out.println("Published: " + id1 + ", " + id2);

        if (latch.await(10, TimeUnit.SECONDS)) {
            System.out.println("Received " + received.get() + " messages over WebSocket.");
        } else {
            System.out.println("Timed out waiting for messages.");
        }

        client.subscribe("retry", message -> {
            System.out.println("[retry] attempt " + message.getDeliveryAttempts());
            client.nack(message.getId(), "retry", Reason.FAILED);
        });
        client.publish("retry", "will be retried until DLQ");

        Thread.sleep(6_000);
        System.out.println("\n=== Network Demo Complete (check /metrics on the server) ===");
        client.close();
    }
}
