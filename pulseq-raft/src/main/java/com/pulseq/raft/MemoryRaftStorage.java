package com.pulseq.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * Storage kept in memory. It outlives the {@link RaftNode} that uses it, so a test can "crash"
 * a node by discarding the node and building a new one over the same storage.
 */
public final class MemoryRaftStorage implements RaftStorage {

    private long currentTerm;
    private int votedFor = NO_VOTE;
    private final List<LogEntry> log = new ArrayList<>();

    @Override
    public long currentTerm() {
        return currentTerm;
    }

    @Override
    public int votedFor() {
        return votedFor;
    }

    @Override
    public void saveTermAndVote(long term, int votedFor) {
        this.currentTerm = term;
        this.votedFor = votedFor;
    }

    @Override
    public long lastIndex() {
        return log.size();
    }

    @Override
    public long termAt(long index) {
        return index == 0 ? 0 : entry(index).term();
    }

    @Override
    public LogEntry entry(long index) {
        return log.get((int) index - 1);
    }

    @Override
    public List<LogEntry> entriesFrom(long fromIndex, int max) {
        if (fromIndex > log.size()) {
            return List.of();
        }
        int from = (int) fromIndex - 1;
        return List.copyOf(log.subList(from, Math.min(log.size(), from + max)));
    }

    @Override
    public void append(LogEntry entry) {
        if (entry.index() != log.size() + 1) {
            throw new IllegalArgumentException("expected index " + (log.size() + 1) + ", got " + entry);
        }
        log.add(entry);
    }

    @Override
    public void truncateFrom(long fromIndex) {
        log.subList((int) fromIndex - 1, log.size()).clear();
    }
}
