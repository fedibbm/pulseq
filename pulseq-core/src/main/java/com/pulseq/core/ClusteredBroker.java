package com.pulseq.core;

import com.pulseq.raft.LeadershipListener;
import com.pulseq.raft.NotLeaderException;
import com.pulseq.raft.RaftRuntime;
import com.pulseq.raft.RaftStorage;
import com.pulseq.raft.Transport;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * One broker of a cluster. All brokers keep the same message store through Raft; only the
 * leader serves producers and consumers. When the leader is lost, another broker takes over
 * with every message that was not yet acknowledged.
 *
 * <p>The queues live in memory on the leader only. Each time this broker becomes the leader
 * it builds them afresh from its store, and it discards them when it stops being the leader,
 * because they may hold changes that never reached a majority.</p>
 *
 * <p>Calls made while this broker is not the leader fail with {@link NotLeaderException},
 * which names the leader when it is known. A leader cut off from the other brokers keeps
 * believing it leads until it hears from them; its writes fail with
 * {@link ReplicationException} meanwhile.</p>
 */
public final class ClusteredBroker implements AutoCloseable {

    /** What exists only while this broker leads. */
    private record Session(QueueManager queues, Dispatcher dispatcher, VisibilityTimeoutChecker checker) {
        void stop() {
            checker.stop();
            dispatcher.shutdown();
        }
    }

    private record Subscription(String topic, String groupId, MessageListener listener) {
    }

    private final int id;
    private final BrokerConfig config;
    private final ReplicatedMessageStore store;
    private final RaftRuntime raft;
    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();
    private final ExecutorService lifecycle;
    private volatile Session session;

    public ClusteredBroker(int id, List<Integer> members, MessageStore localStore,
                           RaftStorage raftStorage, Transport transport, BrokerConfig config,
                           ClusterConfig cluster) {
        this.id = id;
        this.config = config;
        this.store = new ReplicatedMessageStore(localStore, cluster.commitTimeoutMillis());
        // Taking over reads the whole store, far too slow for the Raft thread, which must keep
        // sending heartbeats. Leadership changes are handled in order on a thread of their own.
        this.lifecycle = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "pulseq-broker-" + id);
            thread.setDaemon(true);
            return thread;
        });
        this.raft = new RaftRuntime(id, members, raftStorage, store, cluster.raft(), transport,
                new LeadershipListener() {
                    @Override
                    public void onLeaderReady(long term) {
                        submit(ClusteredBroker.this::takeOver);
                    }

                    @Override
                    public void onLeadershipLost() {
                        submit(ClusteredBroker.this::stepAside);
                    }
                }, cluster.tickMillis());
        this.store.attach(raft);
    }

    public void start() {
        raft.start();
    }

    // ------------------------------------------------------------------ leadership changes

    private void takeOver() {
        QueueManager queues = new QueueManager(store, config);
        queues.recover(false);
        Dispatcher dispatcher = new Dispatcher(queues, config.getConsumerThreads());
        VisibilityTimeoutChecker checker =
                new VisibilityTimeoutChecker(queues, config.getTimeoutCheckRateMillis());
        // Published before the subscriptions are replayed, so a subscribe() racing with the
        // takeover lands on this session either way; subscribing twice is harmless.
        session = new Session(queues, dispatcher, checker);
        for (Subscription subscription : subscriptions) {
            dispatcher.subscribe(subscription.topic(), subscription.groupId(), subscription.listener());
        }
        checker.start();
    }

    private void stepAside() {
        Session ended = session;
        session = null;
        if (ended != null) {
            ended.stop();
        }
    }

    private void submit(Runnable change) {
        try {
            lifecycle.execute(() -> {
                try {
                    change.run();
                } catch (RuntimeException e) {
                    System.err.println("PulseQ: broker " + id + " leadership change failed: " + e);
                }
            });
        } catch (RejectedExecutionException closed) {
            // The broker is shutting down.
        }
    }

    // ------------------------------------------------------------------ producers and consumers

    /**
     * Publishes a message. It returns once a majority of brokers hold the message.
     *
     * @return false when the message id was already seen and the message was dropped
     * @throws NotLeaderException   when this broker is not the leader
     * @throws ReplicationException when no majority confirmed in time; the outcome is unknown
     *                              and the publish should be retried with the same id
     */
    public boolean publish(String topic, Message message) {
        return queues().publish(topic, message);
    }

    /** Acknowledges a delivered message. Returns false when the message is not in flight here. */
    public boolean ack(String topic, String messageId) {
        MessageQueue queue = queues().getQueue(topic);
        return queue != null && queue.ack(messageId);
    }

    /** Rejects or fails a delivered message. Returns false when the message is not in flight here. */
    public boolean nack(String topic, String messageId, Reason reason) {
        MessageQueue queue = queues().getQueue(topic);
        return queue != null && queue.nack(messageId, reason);
    }

    /**
     * Subscribes a listener. It can be called on any broker at any time: the listener receives
     * messages whenever this broker leads, and nothing while it does not.
     *
     * @param groupId consumer group, or null to receive every message
     */
    public void subscribe(String topic, String groupId, MessageListener listener) {
        subscriptions.add(new Subscription(topic, groupId, listener));
        Session current = session;
        if (current != null) {
            current.dispatcher().subscribe(topic, groupId, listener);
        }
    }

    public void unsubscribe(String topic, MessageListener listener) {
        subscriptions.removeIf(s -> s.topic().equals(topic) && s.listener() == listener);
        Session current = session;
        if (current != null) {
            current.dispatcher().unsubscribe(topic, listener);
        }
    }

    /**
     * The queues of the current leadership.
     *
     * @throws NotLeaderException when this broker is not the leader, or has not finished taking over
     */
    public QueueManager queues() {
        Session current = session;
        if (current == null) {
            throw new NotLeaderException(raft.isLeader() ? id : raft.leaderId());
        }
        return current.queues();
    }

    // ------------------------------------------------------------------ state

    public int id() {
        return id;
    }

    /** True once this broker leads and has rebuilt its queues. */
    public boolean isServing() {
        return session != null;
    }

    /** The broker believed to lead, or {@link com.pulseq.raft.RaftNode#NO_LEADER}. */
    public int leaderId() {
        return raft.leaderId();
    }

    /** The Raft node, for connecting this broker to a network. */
    public RaftRuntime raft() {
        return raft;
    }

    /** Stops the broker, like a crash: its stores keep what was written. */
    @Override
    public void close() {
        raft.close(); // reports the loss of leadership, which queues stepAside
        lifecycle.shutdown();
        try {
            lifecycle.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
