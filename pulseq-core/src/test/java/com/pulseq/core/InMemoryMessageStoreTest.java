package com.pulseq.core;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryMessageStoreTest {

    private static final long NOW = System.currentTimeMillis();

    private static Message terminal(String id, long publishedAt, MessageStatus status) {
        return new Message(id, "t", Payloads.toBytes(id), publishedAt, 0, 1, 3, status, 0);
    }

    @Test
    void loadDeadLetteredReturnsOnlyDeadLetteredOldestFirst() {
        InMemoryMessageStore store = new InMemoryMessageStore();
        Message first = terminal("d1", NOW - 100, MessageStatus.DEAD_LETTERED);
        Message second = terminal("d2", NOW - 50, MessageStatus.DEAD_LETTERED);
        store.save(terminal("a1", NOW - 200, MessageStatus.ACKNOWLEDGED));
        store.save(terminal("x1", NOW - 200, MessageStatus.EXPIRED));
        store.save(first);
        store.save(second);

        List<Message> deadLettered = store.loadDeadLettered();

        assertEquals(List.of("d1", "d2"), deadLettered.stream().map(Message::getId).toList(),
                "only dead-lettered messages, ordered oldest first");
    }

    @Test
    void sweepRemovesOnlyOldTerminalMessages() {
        InMemoryMessageStore store = new InMemoryMessageStore();
        long cutoff = NOW - 1_000;
        store.save(terminal("old-acked", cutoff - 100, MessageStatus.ACKNOWLEDGED));
        store.save(terminal("old-dead", cutoff - 100, MessageStatus.DEAD_LETTERED));
        store.save(terminal("old-expired", cutoff - 100, MessageStatus.EXPIRED));
        store.save(terminal("new-acked", cutoff + 100, MessageStatus.ACKNOWLEDGED));
        store.save(terminal("available", cutoff - 100, MessageStatus.AVAILABLE));

        int removed = store.sweepCompleted(cutoff);

        assertEquals(3, removed, "old terminal messages are swept");
        assertTrue(store.loadAllAvailable().stream().anyMatch(m -> m.getId().equals("available")),
                "AVAILABLE messages must never be swept");
    }
}
