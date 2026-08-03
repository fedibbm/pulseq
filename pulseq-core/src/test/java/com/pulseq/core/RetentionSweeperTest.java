package com.pulseq.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RetentionSweeperTest extends BrokerTestSupport {

    @Test
    void periodicSweepPurgesCompletedMessages() throws InterruptedException {
        InMemoryMessageStore store = new InMemoryMessageStore();
        long now = System.currentTimeMillis();
        store.save(new Message("old", "t", Payloads.toBytes("old"),
                now - 3_600_000, 0, 0, 3, MessageStatus.DEAD_LETTERED, 0));

        RetentionSweeper sweeper = new RetentionSweeper(store, 3_600_000, 50);
        sweeper.start();
        try {
            waitFor(() -> store.loadDeadLettered().isEmpty(), 2_000);
        } finally {
            sweeper.stop();
        }
    }

    @Test
    void zeroRetentionDisablesSweeping() {
        InMemoryMessageStore store = new InMemoryMessageStore();
        long now = System.currentTimeMillis();
        store.save(new Message("keep", "t", Payloads.toBytes("keep"),
                now - 3_600_000, 0, 0, 3, MessageStatus.DEAD_LETTERED, 0));

        RetentionSweeper sweeper = new RetentionSweeper(store, 0, 50);
        sweeper.start();
        try {
            sleep(300);
            assertEquals(1, store.loadDeadLettered().size(), "disabled retention must not sweep");
        } finally {
            sweeper.stop();
        }
    }
}
