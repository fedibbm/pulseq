package com.pulseq.core;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class VisibilityTimeoutChecker {
    private QueueManager queueManager;
    private ScheduledExecutorService scheduler;
    private static final int RATE = 5;

    public VisibilityTimeoutChecker(QueueManager queueManager) {
        this.queueManager = queueManager;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() {
        this.scheduler.scheduleAtFixedRate(this::checkAndRequeue, 0, RATE, TimeUnit.SECONDS);
    }

    public void stop() {
        scheduler.shutdown();
    }

    void checkAndRequeue() {
        for (String topic : queueManager.listTopics()) {
            try {
                MessageQueue queue = this.queueManager.getQueue(topic);
                queue.requeueTimedOut();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
