package com.pulseq.core;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DeadLetterQueueTest {

    @Test
    void addListSizePeekAndRemove() {
        DeadLetterQueue dlq = new DeadLetterQueue("t");
        Message m1 = message("1");
        Message m2 = message("2");

        dlq.add(m1);
        dlq.add(m2);

        assertEquals("t", dlq.getSourceTopic());
        assertEquals(2, dlq.size());
        assertEquals(m1, dlq.peek(), "peek returns oldest without removing");
        assertEquals(2, dlq.size());
        assertEquals(m1, dlq.remove(), "remove returns oldest");
        assertEquals(1, dlq.size());

        List<Message> remaining = dlq.list();
        assertEquals(1, remaining.size());
        assertEquals("2", remaining.get(0).getId());
    }

    @Test
    void peekAndRemoveReturnNullWhenEmpty() {
        DeadLetterQueue dlq = new DeadLetterQueue("t");
        assertNull(dlq.peek());
        assertNull(dlq.remove());
        assertEquals(0, dlq.size());
    }

    private static Message message(String id) {
        return new Message(id, "t", Payloads.toBytes(id));
    }
}
