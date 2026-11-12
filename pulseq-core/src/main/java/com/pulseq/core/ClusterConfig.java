package com.pulseq.core;

import com.pulseq.raft.RaftConfig;

/**
 * Timing of a broker cluster.
 *
 * @param raft                election and heartbeat timing, in ticks
 * @param tickMillis          length of one tick
 * @param commitTimeoutMillis how long a write waits for a majority of brokers before failing
 */
public record ClusterConfig(RaftConfig raft, long tickMillis, long commitTimeoutMillis) {

    /**
     * Elections after 0.5 to 1 s without a leader, heartbeats every 100 ms. The election
     * timeout must stay well above the longest pause a healthy broker can have (a disk sync,
     * a garbage collection), or brokers depose a leader that is merely slow.
     */
    public static ClusterConfig defaults() {
        return new ClusterConfig(new RaftConfig(50, 100, 10, 64), 10, 5_000);
    }
}
