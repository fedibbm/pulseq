package com.pulseq.raft;

/** A message on its way from one node to another. */
public record Envelope(int from, int to, RaftMessage message) {
}
