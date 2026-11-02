package com.pulseq.raft;

import java.util.List;

/** The four Raft RPC messages. Every message carries the sender's term. */
public sealed interface RaftMessage {

    long term();

    record RequestVote(long term, int candidateId, long lastLogIndex, long lastLogTerm)
            implements RaftMessage {
    }

    record RequestVoteResponse(long term, boolean granted) implements RaftMessage {
    }

    record AppendEntries(long term, int leaderId, long prevLogIndex, long prevLogTerm,
                         List<LogEntry> entries, long leaderCommit) implements RaftMessage {
        public AppendEntries {
            entries = List.copyOf(entries);
        }
    }

    /**
     * @param matchIndex    on success, the last index known to match the leader's log
     * @param conflictIndex on failure, the index the leader should retry from
     */
    record AppendEntriesResponse(long term, boolean success, long matchIndex, long conflictIndex)
            implements RaftMessage {
    }
}
