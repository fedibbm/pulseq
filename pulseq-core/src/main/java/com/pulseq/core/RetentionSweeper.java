package com.pulseq.core;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically purges completed messages (acknowledged, dead-lettered, expired) that are
 * older than the retention window, keeping the store from growing unbounded.
 *
 * <p>This mirrors the retention model used by Kafka and Pulsar: finished messages are kept
 * for a bounded window and then swept away. Recovery semantics are untouched — only terminal
 * messages are removed, never available or in-flight work.</p>
 */
public class RetentionSweeper {

    private final MessageStore store;
    private final long retentionMillis;
    private final ScheduledExecutorService scheduler;
    private final long rateMillis;

    public RetentionSweeper(MessageStore store, long retentionMillis, long rateMillis) {
        this.store = store;
        this.retentionMillis = retentionMillis;
        this.rateMillis = Math.max(1, rateMillis);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pulseq-retention-sweeper");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        if (retentionMillis <= 0) return;
        scheduler.scheduleAtFixedRate(this::sweep, rateMillis, rateMillis, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    public boolean isStopped() {
        return scheduler.isShutdown();
    }

    void sweep() {
        try {
            int removed = store.sweepCompleted(System.currentTimeMillis() - retentionMillis);
            if (removed > 0) {
                System.out.println("PulseQ: retention sweep removed " + removed + " completed message(s)");
            }
        } catch (Exception e) {
            System.err.println("PulseQ: retention sweep failed: " + e);
        }
    }
}
