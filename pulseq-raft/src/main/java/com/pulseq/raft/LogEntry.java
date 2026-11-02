package com.pulseq.raft;

import java.util.Arrays;

/**
 * One entry in the replicated log. Indexes start at 1. A no-op entry carries no command: a new
 * leader appends one so that entries from earlier terms can be committed.
 */
public record LogEntry(long index, long term, boolean noop, byte[] command) {

    public static LogEntry command(long index, long term, byte[] command) {
        return new LogEntry(index, term, false, command);
    }

    public static LogEntry noop(long index, long term) {
        return new LogEntry(index, term, true, new byte[0]);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof LogEntry e && index == e.index && term == e.term && noop == e.noop
                && Arrays.equals(command, e.command);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * Long.hashCode(index) + Long.hashCode(term)) + Arrays.hashCode(command);
    }

    @Override
    public String toString() {
        return "LogEntry[" + index + "@" + term + (noop ? " noop" : " " + command.length + "b") + "]";
    }
}
