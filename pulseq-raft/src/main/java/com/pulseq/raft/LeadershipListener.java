package com.pulseq.raft;

/**
 * Told when this node may start and must stop acting as the leader. Both methods run on the
 * node's own thread, so they must return quickly.
 */
public interface LeadershipListener {

    /**
     * This node is the leader of the term and its state machine has applied every command
     * committed by earlier leaders. Only now is the state machine a complete base to serve from.
     */
    void onLeaderReady(long term);

    /** This node is no longer the leader. Called only after a matching {@link #onLeaderReady}. */
    void onLeadershipLost();
}
