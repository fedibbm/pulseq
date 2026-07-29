package com.pulseq.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Delivers messages from topic queues to subscribed consumers.
 *
 * <p>One daemon consumer thread per topic drains the queue; each message is then dispatched
 * asynchronously to the topic's listeners:</p>
 *
 * <ul>
 *   <li>Listeners subscribed <em>without</em> a consumer group receive every message (fan-out).</li>
 *   <li>Listeners subscribed with the <em>same</em> consumer group share messages between them
 *       (competing consumers, round-robin).</li>
 * </ul>
 *
 * <p>Delivery runs on a bounded worker pool. When the pool saturates, the caller-runs policy
 * applies backpressure to the consumer thread so slow consumers do not buffer unbounded work.
 * A slow or failing listener therefore never blocks other listeners on the same topic.</p>
 */
public class Dispatcher {

    private final Map<String, List<Subscription>> subscriptions = new ConcurrentHashMap<>();
    private final Map<String, Thread> consumerThreads = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> groupCounters = new ConcurrentHashMap<>();
    private final QueueManager queueManager;
    private final ExecutorService executor;

    public Dispatcher(QueueManager queueManager) {
        this(queueManager, BrokerConfig.defaults().getConsumerThreads());
    }

    public Dispatcher(QueueManager queueManager, int consumerThreads) {
        this.queueManager = queueManager;
        this.executor = new ThreadPoolExecutor(
                consumerThreads, consumerThreads,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1024),
                r -> {
                    Thread t = new Thread(r, "pulseq-dispatcher");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void subscribe(String topic, MessageListener listener) {
        subscribe(topic, null, listener);
    }

    /**
     * Subscribes a listener to a topic, optionally joining a consumer group.
     *
     * @param groupId consumer group, or null for fan-out delivery
     */
    public void subscribe(String topic, String groupId, MessageListener listener) {
        queueManager.createQueue(topic);
        subscriptions.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>())
                .add(new Subscription(groupId, listener));
        startConsumerIfNeeded(topic);
    }

    /**
     * Removes a listener from a topic. When the last listener is removed the topic's consumer
     * thread is stopped. No-op when the listener was not subscribed.
     *
     * @param topic   the topic to leave
     * @param session the listener to remove
     */
    public void unsubscribe(String topic, MessageListener session) {
        List<Subscription> listeners = subscriptions.get(topic);
        if (listeners == null) return;
        listeners.removeIf(sub -> sub.listener() == session);
        if (listeners.isEmpty()) {
            subscriptions.remove(topic);
            Thread consumerThread = consumerThreads.get(topic);
            if (consumerThread != null) {
                consumerThread.interrupt();
                consumerThreads.remove(topic);
            }
        }
    }

    public void onSessionClosed(MessageListener session) {
        for (String topic : new ArrayList<>(subscriptions.keySet())) {
            unsubscribe(topic, session);
        }
    }

    /**
     * Stops all consumer threads and shuts down the dispatch workers.
     */
    public void shutdown() {
        for (Thread thread : new ArrayList<>(consumerThreads.values())) {
            thread.interrupt();
        }
        consumerThreads.clear();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    private void startConsumerIfNeeded(String topic) {
        if (consumerThreads.containsKey(topic)) return;
        Thread consumer = new Thread(() -> consume(topic), "pulseq-consumer-" + topic);
        consumer.setDaemon(true);
        if (consumerThreads.putIfAbsent(topic, consumer) == null) {
            consumer.start();
        }
    }

    private void consume(String topic) {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                MessageQueue queue = queueManager.getQueue(topic);
                if (queue == null) return;
                Message message = queue.dequeue();
                if (message == null) return;
                dispatch(topic, message);
            }
        } finally {
            consumerThreads.remove(topic, Thread.currentThread());
        }
    }

    void dispatch(String topic, Message message) {
        List<Subscription> subs = subscriptions.get(topic);
        if (subs == null || subs.isEmpty()) return;

        Map<String, List<Subscription>> byGroup = new LinkedHashMap<>();
        for (Subscription sub : subs) {
            byGroup.computeIfAbsent(sub.groupId() == null ? "" : sub.groupId(), k -> new ArrayList<>()).add(sub);
        }
        for (Map.Entry<String, List<Subscription>> entry : byGroup.entrySet()) {
            List<Subscription> members = entry.getValue();
            if (entry.getKey().isEmpty()) {
                for (Subscription sub : members) {
                    executor.execute(() -> deliver(sub.listener(), message));
                }
            } else {
                AtomicInteger counter = groupCounters.computeIfAbsent(entry.getKey(), k -> new AtomicInteger());
                Subscription chosen = members.get(Math.floorMod(counter.getAndIncrement(), members.size()));
                executor.execute(() -> deliver(chosen.listener(), message));
            }
        }
    }

    private void deliver(MessageListener listener, Message message) {
        try {
            listener.onMessage(message);
        } catch (Throwable t) {
            System.err.println("PulseQ: consumer threw for topic '" + message.getTopic() + "': " + t);
        }
    }

    private record Subscription(String groupId, MessageListener listener) {
    }
}
