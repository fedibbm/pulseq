package com.pulseq.raft;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/** Binary encoding of log entries and messages, for the disk log and the network. */
public final class RaftCodec {

    private static final byte REQUEST_VOTE = 1;
    private static final byte REQUEST_VOTE_RESPONSE = 2;
    private static final byte APPEND_ENTRIES = 3;
    private static final byte APPEND_ENTRIES_RESPONSE = 4;

    private RaftCodec() {
    }

    public static byte[] encode(LogEntry entry) {
        return write(out -> writeEntry(out, entry));
    }

    public static LogEntry decodeEntry(byte[] bytes) {
        return read(bytes, RaftCodec::readEntry);
    }

    public static byte[] encode(Envelope envelope) {
        return write(out -> {
            out.writeInt(envelope.from());
            out.writeInt(envelope.to());
            RaftMessage message = envelope.message();
            if (message instanceof RaftMessage.RequestVote m) {
                out.writeByte(REQUEST_VOTE);
                out.writeLong(m.term());
                out.writeInt(m.candidateId());
                out.writeLong(m.lastLogIndex());
                out.writeLong(m.lastLogTerm());
            } else if (message instanceof RaftMessage.RequestVoteResponse m) {
                out.writeByte(REQUEST_VOTE_RESPONSE);
                out.writeLong(m.term());
                out.writeBoolean(m.granted());
            } else if (message instanceof RaftMessage.AppendEntries m) {
                out.writeByte(APPEND_ENTRIES);
                out.writeLong(m.term());
                out.writeInt(m.leaderId());
                out.writeLong(m.prevLogIndex());
                out.writeLong(m.prevLogTerm());
                out.writeLong(m.leaderCommit());
                out.writeInt(m.entries().size());
                for (LogEntry entry : m.entries()) {
                    writeEntry(out, entry);
                }
            } else if (message instanceof RaftMessage.AppendEntriesResponse m) {
                out.writeByte(APPEND_ENTRIES_RESPONSE);
                out.writeLong(m.term());
                out.writeBoolean(m.success());
                out.writeLong(m.matchIndex());
                out.writeLong(m.conflictIndex());
            }
        });
    }

    public static Envelope decodeEnvelope(byte[] bytes) {
        return read(bytes, in -> {
            int from = in.readInt();
            int to = in.readInt();
            byte type = in.readByte();
            long term = in.readLong();
            RaftMessage message = switch (type) {
                case REQUEST_VOTE -> new RaftMessage.RequestVote(term, in.readInt(), in.readLong(), in.readLong());
                case REQUEST_VOTE_RESPONSE -> new RaftMessage.RequestVoteResponse(term, in.readBoolean());
                case APPEND_ENTRIES -> {
                    int leaderId = in.readInt();
                    long prevLogIndex = in.readLong();
                    long prevLogTerm = in.readLong();
                    long leaderCommit = in.readLong();
                    int count = in.readInt();
                    List<LogEntry> entries = new ArrayList<>(Math.min(count, 1024));
                    for (int i = 0; i < count; i++) {
                        entries.add(readEntry(in));
                    }
                    yield new RaftMessage.AppendEntries(term, leaderId, prevLogIndex, prevLogTerm,
                            entries, leaderCommit);
                }
                case APPEND_ENTRIES_RESPONSE ->
                        new RaftMessage.AppendEntriesResponse(term, in.readBoolean(), in.readLong(), in.readLong());
                default -> throw new IOException("unknown message type " + type);
            };
            return new Envelope(from, to, message);
        });
    }

    private static void writeEntry(DataOutputStream out, LogEntry entry) throws IOException {
        out.writeLong(entry.index());
        out.writeLong(entry.term());
        out.writeBoolean(entry.noop());
        out.writeInt(entry.command().length);
        out.write(entry.command());
    }

    private static LogEntry readEntry(DataInputStream in) throws IOException {
        long index = in.readLong();
        long term = in.readLong();
        boolean noop = in.readBoolean();
        int length = in.readInt();
        byte[] command = in.readNBytes(length);
        if (command.length != length) {
            throw new IOException("entry " + index + " is cut short");
        }
        return new LogEntry(index, term, noop, command);
    }

    private interface Writer {
        void write(DataOutputStream out) throws IOException;
    }

    private interface Reader<T> {
        T read(DataInputStream in) throws IOException;
    }

    private static byte[] write(Writer writer) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writer.write(out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }

    private static <T> T read(byte[] bytes, Reader<T> reader) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            return reader.read(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
