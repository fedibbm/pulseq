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

    /** Elections after 100 to 200 ms without a leader, heartbeats every 20 ms. */
    public static ClusterConfig defaults() {
        return new ClusterConfig(RaftConfig.defaults(), 10, 2_000);
    }
}
