package com.pulseq.core;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PayloadsTest {

    @Test
    void encodesStringsNumbersBooleansNullMapsAndLists() {
        java.util.Map<String, Object> value = new java.util.HashMap<>();
        value.put("name", "pulse");
        value.put("count", 3);
        value.put("ok", true);
        value.put("tags", List.of("a", "b"));
        value.put("nothing", null);

        String json = Payloads.toJson(value);

        assertTrue(json.contains("\"name\":\"pulse\""));
        assertTrue(json.contains("\"count\":3"));
        assertTrue(json.contains("\"ok\":true"));
        assertTrue(json.contains("\"tags\":[\"a\",\"b\"]"));
        assertTrue(json.contains("\"nothing\":null"));
    }

    @Test
    void escapesQuotesAndNewlines() {
        String json = Payloads.toJson(Map.of("text", "say \"hi\"\nbye"));
        assertTrue(json.contains("say \\\"hi\\\"\\nbye"));
    }

    @Test
    void stringRoundTripThroughMessage() {
        Message m = Message.delivery("1", "t", Payloads.toBytes("hello"), 0);
        assertEquals("hello", Payloads.toString(m));
        assertEquals("hello", m.getPayloadAsString());
    }

    @Test
    void rejectsUnsupportedTypes() {
        assertThrows(IllegalArgumentException.class, () -> Payloads.toJson(new Object()));
    }
}
