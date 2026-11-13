package com.pulseq.core;

/**
 * What a server needs from a broker, whether it runs alone or as one of a cluster.
 */
public interface Broker {

    /**
     * The queues to publish to and acknowledge on.
     *
     * @throws com.pulseq.raft.NotLeaderException in a cluster, when this broker is not the leader
     */
    QueueManager queues();

    /**
     * Subscribes a listener to a topic.
     *
     * @param groupId consumer group, or null to receive every message
     */
    void subscribe(String topic, String groupId, MessageListener listener);

    /** Removes a listener from every topic it subscribed to. */
    void unsubscribe(MessageListener listener);

    /** True when this broker serves producers and consumers: always alone, only as leader in a cluster. */
    boolean isServing();
}
