package com.pulseq.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileRaftStorageTest {

    @TempDir
    Path directory;

    @Test
    void entriesTermAndVoteSurviveReopening() {
        try (FileRaftStorage storage = new FileRaftStorage(directory)) {
            storage.saveTermAndVote(7, 2);
            storage.append(LogEntry.noop(1, 5));
            storage.append(LogEntry.command(2, 5, "a".getBytes()));
            storage.append(LogEntry.command(3, 7, "bb".getBytes()));
        }
        try (FileRaftStorage reopened = new FileRaftStorage(directory)) {
            assertEquals(7, reopened.currentTerm());
            assertEquals(2, reopened.votedFor());
            assertEquals(3, reopened.lastIndex());
            assertEquals(LogEntry.noop(1, 5), reopened.entry(1));
            assertEquals(LogEntry.command(3, 7, "bb".getBytes()), reopened.entry(3));
            assertEquals(List.of(reopened.entry(2), reopened.entry(3)), reopened.entriesFrom(2, 10));
            assertEquals(7, reopened.termAt(3));
        }
    }

    @Test
    void freshDirectoryStartsEmpty() {
        try (FileRaftStorage storage = new FileRaftStorage(directory.resolve("new"))) {
            assertEquals(0, storage.currentTerm());
            assertEquals(RaftStorage.NO_VOTE, storage.votedFor());
            assertEquals(0, storage.lastIndex());
            assertEquals(0, storage.termAt(0));
        }
    }

    @Test
    void truncationSurvivesReopeningAndTheLogCanGrowAgain() {
        try (FileRaftStorage storage = new FileRaftStorage(directory)) {
            for (int i = 1; i <= 5; i++) {
                storage.append(LogEntry.command(i, 1, ("old" + i).getBytes()));
            }
            storage.truncateFrom(3);
            storage.append(LogEntry.command(3, 2, "new3".getBytes()));
        }
        try (FileRaftStorage reopened = new FileRaftStorage(directory)) {
            assertEquals(3, reopened.lastIndex());
            assertEquals(LogEntry.command(2, 1, "old2".getBytes()), reopened.entry(2));
            assertEquals(LogEntry.command(3, 2, "new3".getBytes()), reopened.entry(3));
        }
    }

    @Test
    void halfWrittenRecordAtTheEndIsDiscarded() throws IOException {
        long sizeAfterTwo;
        try (FileRaftStorage storage = new FileRaftStorage(directory)) {
            storage.append(LogEntry.command(1, 1, "a".getBytes()));
            storage.append(LogEntry.command(2, 1, "b".getBytes()));
            storage.flush();
            sizeAfterTwo = Files.size(directory.resolve("log"));
            storage.append(LogEntry.command(3, 1, "a longer command".getBytes()));
        }
        // A crash in the middle of writing entry 3: only part of its record reached the disk.
        try (FileChannel log = FileChannel.open(directory.resolve("log"), StandardOpenOption.WRITE)) {
            log.truncate(sizeAfterTwo + 11);
        }
        try (FileRaftStorage reopened = new FileRaftStorage(directory)) {
            assertEquals(2, reopened.lastIndex());
            reopened.append(LogEntry.command(3, 2, "retry".getBytes()));
        }
        try (FileRaftStorage again = new FileRaftStorage(directory)) {
            assertEquals(LogEntry.command(3, 2, "retry".getBytes()), again.entry(3));
            assertEquals(sizeAfterTwo, firstRecordsSize(again, 2));
        }
    }

    @Test
    void corruptRecordCutsTheLogThere() throws IOException {
        long sizeAfterOne;
        try (FileRaftStorage storage = new FileRaftStorage(directory)) {
            storage.append(LogEntry.command(1, 1, "a".getBytes()));
            storage.flush();
            sizeAfterOne = Files.size(directory.resolve("log"));
            storage.append(LogEntry.command(2, 1, "b".getBytes()));
            storage.append(LogEntry.command(3, 1, "c".getBytes()));
        }
        try (FileChannel log = FileChannel.open(directory.resolve("log"), StandardOpenOption.WRITE)) {
            log.write(ByteBuffer.wrap(new byte[] {0x55}), sizeAfterOne + 12); // flip a byte in entry 2
        }
        try (FileRaftStorage reopened = new FileRaftStorage(directory)) {
            assertEquals(1, reopened.lastIndex(), "nothing after a damaged record can be trusted");
        }
    }

    private long firstRecordsSize(FileRaftStorage storage, int count) {
        long size = 0;
        for (int i = 1; i <= count; i++) {
            size += 8 + RaftCodec.encode(storage.entry(i)).length;
        }
        return size;
    }
}
