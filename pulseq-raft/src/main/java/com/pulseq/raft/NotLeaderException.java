package com.pulseq.raft;

/**
 * The command was not committed because this node is not the leader, or stopped being the
 * leader before the command reached a majority. The command had no effect and can be retried
 * on the current leader.
 */
public class NotLeaderException extends RuntimeException {

    private final int leaderId;

    public NotLeaderException(int leaderId) {
        super(leaderId == RaftNode.NO_LEADER ? "not the leader; no leader known"
                : "not the leader; try node " + leaderId);
        this.leaderId = leaderId;
    }

    /** The node believed to be the leader, or {@link RaftNode#NO_LEADER}. */
    public int leaderId() {
        return leaderId;
    }
}
