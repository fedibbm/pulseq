package com.pulseq.sdk;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Demonstrates automatic reconnection: subscribe first, then publish continuously while
 * stopping/starting the server. The client reconnects with exponential backoff and resumes
 * consuming once the broker is back.
 */
public class ReconnectDemo {

    public static void main(String[] args) throws InterruptedException {
        String baseUrl = args.length > 0 ? args[0] : "http://localhost:8080";
        System.out.println("=== PulseQ Reconnect Demo (" + baseUrl + ") ===");
        System.out.println("Restart the server while this runs to watch the client reconnect.\n");

        PulseQClient client = PulseQClient.connect(baseUrl);
        AtomicInteger received = new AtomicInteger();

        client.subscribe("resilient", message -> {
            received.incrementAndGet();
            client.ack(message.getId(), "resilient");
        });

        for (int i = 0; i < 120; i++) {
            try {
                client.publish("resilient", "msg-" + i);
                Thread.sleep(1_000);
            } catch (Exception e) {
                System.out.println("[" + System.currentTimeMillis() + "] publish failed, broker may be down: "
                        + e.getMessage());
                Thread.sleep(2_000);
            }
        }
        System.out.println("Total received: " + received.get());
        client.close();
    }
}
