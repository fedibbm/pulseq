package com.pulseq.raft;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RaftCodecTest {

    @Test
    void everyMessageTypeSurvivesARoundTrip() {
        List<RaftMessage> messages = List.of(
                new RaftMessage.RequestVote(9, 2, 41, 8),
                new RaftMessage.RequestVoteResponse(9, true),
                new RaftMessage.AppendEntries(9, 1, 40, 8, List.of(
                        LogEntry.noop(41, 9),
                        LogEntry.command(42, 9, new byte[] {0, 1, 2, -1}),
                        LogEntry.command(43, 9, new byte[0])), 39),
                new RaftMessage.AppendEntries(9, 1, 43, 9, List.of(), 43),
                new RaftMessage.AppendEntriesResponse(9, false, 0, 17));
        for (RaftMessage message : messages) {
            Envelope envelope = new Envelope(1, 2, message);
            assertEquals(envelope, RaftCodec.decodeEnvelope(RaftCodec.encode(envelope)));
        }
    }
}
