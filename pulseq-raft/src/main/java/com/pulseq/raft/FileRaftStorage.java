package com.pulseq.raft;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Raft state on disk, in a directory holding two files.
 *
 * <p>{@code log} is append-only: one record per entry, each a length, a checksum and the
 * encoded entry. A crash can leave a half-written record at the end; on opening, the log is
 * read up to the first record that is incomplete or fails its checksum and cut there. Such a
 * record was never flushed, so no promise was made about it.</p>
 *
 * <p>{@code meta} holds the term and vote. It is replaced atomically (write a new file, sync
 * it, rename it over the old one), so a crash leaves either the old or the new value.</p>
 *
 * <p>Entries are also kept in memory, so reads never touch the disk. There is no log
 * compaction yet: both the file and the memory grow with the log.</p>
 */
public final class FileRaftStorage implements RaftStorage, AutoCloseable {

    private static final int HEADER_BYTES = 8; // length + checksum
    private static final int MAX_RECORD_BYTES = 64 * 1024 * 1024;

    private final Path metaFile;
    private final Path metaTemp;
    private final FileChannel log;
    private final List<LogEntry> entries = new ArrayList<>();
    private final List<Long> offsets = new ArrayList<>();
    private long endOffset;
    private boolean unflushed;
    private long currentTerm;
    private int votedFor = NO_VOTE;

    public FileRaftStorage(Path directory) {
        try {
            Files.createDirectories(directory);
            metaFile = directory.resolve("meta");
            metaTemp = directory.resolve("meta.tmp");
            loadMeta();
            log = FileChannel.open(directory.resolve("log"), StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            loadLog();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot open Raft storage in " + directory, e);
        }
    }

    // ------------------------------------------------------------------ term and vote

    private void loadMeta() throws IOException {
        if (!Files.exists(metaFile)) {
            return;
        }
        ByteBuffer meta = ByteBuffer.wrap(Files.readAllBytes(metaFile));
        if (meta.remaining() != 20) {
            throw new IOException(metaFile + " has an unexpected size: " + meta.remaining());
        }
        long term = meta.getLong();
        int vote = meta.getInt();
        if (meta.getLong() != checksum(meta.array(), 0, 12)) {
            throw new IOException(metaFile + " is corrupt");
        }
        currentTerm = term;
        votedFor = vote;
    }

    @Override
    public void saveTermAndVote(long term, int votedFor) {
        ByteBuffer meta = ByteBuffer.allocate(20);
        meta.putLong(term).putInt(votedFor);
        meta.putLong(checksum(meta.array(), 0, 12));
        meta.flip();
        try {
            try (FileChannel out = FileChannel.open(metaTemp, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                while (meta.hasRemaining()) {
                    out.write(meta);
                }
                out.force(true);
            }
            Files.move(metaTemp, metaFile, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot save term and vote", e);
        }
        this.currentTerm = term;
        this.votedFor = votedFor;
    }

    @Override
    public long currentTerm() {
        return currentTerm;
    }

    @Override
    public int votedFor() {
        return votedFor;
    }

    // ------------------------------------------------------------------ log

    private void loadLog() throws IOException {
        long size = log.size();
        long offset = 0;
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
        while (offset + HEADER_BYTES <= size) {
            header.clear();
            readFully(header, offset);
            header.flip();
            int length = header.getInt();
            int expected = header.getInt();
            if (length <= 0 || length > MAX_RECORD_BYTES || offset + HEADER_BYTES + length > size) {
                break; // a record cut short by a crash
            }
            ByteBuffer body = ByteBuffer.allocate(length);
            readFully(body, offset + HEADER_BYTES);
            if ((int) checksum(body.array(), 0, length) != expected) {
                break;
            }
            LogEntry entry = RaftCodec.decodeEntry(body.array());
            if (entry.index() != entries.size() + 1) {
                break;
            }
            entries.add(entry);
            offsets.add(offset);
            offset += HEADER_BYTES + length;
        }
        if (offset < size) {
            log.truncate(offset);
            log.force(true);
        }
        endOffset = offset;
    }

    private void readFully(ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int read = log.read(buffer, position + buffer.position());
            if (read < 0) {
                throw new IOException("unexpected end of log");
            }
        }
    }

    @Override
    public void append(LogEntry entry) {
        if (entry.index() != entries.size() + 1) {
            throw new IllegalArgumentException("expected index " + (entries.size() + 1) + ", got " + entry);
        }
        byte[] body = RaftCodec.encode(entry);
        ByteBuffer record = ByteBuffer.allocate(HEADER_BYTES + body.length);
        record.putInt(body.length).putInt((int) checksum(body, 0, body.length)).put(body);
        record.flip();
        try {
            long position = endOffset;
            while (record.hasRemaining()) {
                position += log.write(record, position);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot append " + entry, e);
        }
        entries.add(entry);
        offsets.add(endOffset);
        endOffset += HEADER_BYTES + body.length;
        unflushed = true;
    }

    @Override
    public void truncateFrom(long fromIndex) {
        int from = (int) fromIndex - 1;
        long offset = offsets.get(from);
        try {
            log.truncate(offset);
            log.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot truncate the log at " + fromIndex, e);
        }
        entries.subList(from, entries.size()).clear();
        offsets.subList(from, offsets.size()).clear();
        endOffset = offset;
    }

    @Override
    public void flush() {
        if (!unflushed) {
            return;
        }
        try {
            log.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot flush the log", e);
        }
        unflushed = false;
    }

    @Override
    public long lastIndex() {
        return entries.size();
    }

    @Override
    public long termAt(long index) {
        return index == 0 ? 0 : entry(index).term();
    }

    @Override
    public LogEntry entry(long index) {
        return entries.get((int) index - 1);
    }

    @Override
    public List<LogEntry> entriesFrom(long fromIndex, int max) {
        if (fromIndex > entries.size()) {
            return List.of();
        }
        int from = (int) fromIndex - 1;
        return List.copyOf(entries.subList(from, Math.min(entries.size(), from + max)));
    }

    @Override
    public void close() {
        try {
            flush();
            log.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long checksum(byte[] bytes, int offset, int length) {
        CRC32 crc = new CRC32();
        crc.update(bytes, offset, length);
        return crc.getValue();
    }
}
