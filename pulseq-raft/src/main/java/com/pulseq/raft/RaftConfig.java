package com.pulseq.raft;

/**
 * Timing, measured in ticks. A follower that hears nothing for a random number of ticks in
 * [electionMinTicks, electionMaxTicks] starts an election; a leader sends a heartbeat every
 * heartbeatTicks.
 */
public record RaftConfig(int electionMinTicks, int electionMaxTicks, int heartbeatTicks,
                         int maxEntriesPerAppend) {

    public RaftConfig {
        if (heartbeatTicks < 1 || electionMinTicks <= heartbeatTicks
                || electionMaxTicks < electionMinTicks || maxEntriesPerAppend < 1) {
            throw new IllegalArgumentException("invalid Raft timing: " + electionMinTicks + ".."
                    + electionMaxTicks + " election, " + heartbeatTicks + " heartbeat");
        }
    }

    public static RaftConfig defaults() {
        return new RaftConfig(10, 20, 2, 64);
    }
}
