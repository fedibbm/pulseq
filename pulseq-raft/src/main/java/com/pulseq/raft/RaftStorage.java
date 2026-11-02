package com.pulseq.raft;

import java.util.List;

/**
 * The state Raft must keep across a crash: the current term, the vote cast in that term, and
 * the log. Every method must be durable before it returns.
 */
public interface RaftStorage {

    int NO_VOTE = -1;

    long currentTerm();

    /** The node voted for in the current term, or {@link #NO_VOTE}. */
    int votedFor();

    void saveTermAndVote(long term, int votedFor);

    /** Index of the last entry, or 0 when the log is empty. */
    long lastIndex();

    /** Term of the entry at the index, or 0 for index 0. */
    long termAt(long index);

    LogEntry entry(long index);

    /** Up to {@code max} entries starting at {@code fromIndex}; empty when past the end. */
    List<LogEntry> entriesFrom(long fromIndex, int max);

    /** Appends an entry whose index must be {@code lastIndex() + 1}. */
    void append(LogEntry entry);

    /** Removes the entry at {@code fromIndex} and everything after it. */
    void truncateFrom(long fromIndex);
}
