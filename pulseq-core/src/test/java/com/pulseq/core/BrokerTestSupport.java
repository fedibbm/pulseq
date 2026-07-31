package com.pulseq.core;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Shared test utilities for the core broker tests.
 */
public abstract class BrokerTestSupport {

    protected static BrokerConfig fastConfig() {
        return new BrokerConfig(10, 100, 50, 5_000, 3, 8, 1_000);
    }

    protected static Message message(String id, String topic) {
        return new Message(id, topic, Payloads.toBytes(id));
    }

    protected static void waitFor(BooleanSupplier condition, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(20);
        }
        fail("condition not met within " + timeoutMillis + " ms");
    }

    protected static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
