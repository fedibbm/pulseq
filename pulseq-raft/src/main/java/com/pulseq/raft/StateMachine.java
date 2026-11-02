package com.pulseq.raft;

/** Receives committed commands, in log order, exactly once per node lifetime. */
public interface StateMachine {
    void apply(long index, byte[] command);
}
