package com.pulseq.core;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically sweeps topic queues for messages whose visibility timeout or retry backoff
 * has elapsed, re-queueing them as appropriate.
 *
 * <p>Only queues that actually have pending timers are touched, so the sweep cost stays flat
 * even as the number of topics grows.</p>
 */
public class VisibilityTimeoutChecker {

    private final QueueManager queueManager;
    private final ScheduledExecutorService scheduler;
    private final long rateMillis;

    public VisibilityTimeoutChecker(QueueManager queueManager) {
        this(queueManager, BrokerConfig.defaults().getTimeoutCheckRateMillis());
    }

    public VisibilityTimeoutChecker(QueueManager queueManager, long rateMillis) {
        this.queueManager = queueManager;
        this.rateMillis = Math.max(1, rateMillis);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pulseq-timeout-checker");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::checkAndRequeue, 0, rateMillis, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    public boolean isStopped() {
        return scheduler.isShutdown();
    }

    void checkAndRequeue() {
        for (String topic : queueManager.listTopics()) {
            try {
                MessageQueue queue = queueManager.getQueue(topic);
                if (queue == null) continue;
                if (queue.delayedCount() > 0) {
                    queue.requeueTimedOut();
                }
                queue.cleanupExpired();
            } catch (Exception e) {
                System.err.println("PulseQ: timeout sweep failed for topic '" + topic + "': " + e);
            }
        }
    }
}
