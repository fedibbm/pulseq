package com.pulseq.raft;

/** Carries messages to other nodes. Delivery may fail silently: Raft retries on its own. */
public interface Transport {
    void send(Envelope envelope);
}
