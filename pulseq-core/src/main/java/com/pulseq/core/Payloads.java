package com.pulseq.core;

import java.util.List;
import java.util.Map;

/**
 * Small serialization helpers for message payloads.
 *
 * <p>Payloads are stored as raw bytes; these helpers convert between bytes, strings and a
 * minimal JSON encoder for structured payloads (useful for the REST and WebSocket API).</p>
 */
public final class Payloads {

    private Payloads() {
    }

    public static byte[] toBytes(String value) {
        return value == null ? new byte[0] : value.getBytes();
    }

    public static String toString(Message message) {
        return new String(message.getPayload());
    }

    public static byte[] json(Object value) {
        return toBytes(toJson(value));
    }

    /**
     * Encodes common Java values (String, Number, Boolean, null, Map, Iterable) to JSON.
     *
     * @throws IllegalArgumentException when the value contains an unsupported type
     */
    public static String toJson(Object value) {
        StringBuilder sb = new StringBuilder();
        appendJson(sb, value);
        return sb.toString();
    }

    private static void appendJson(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof String s) {
            appendString(sb, s);
        } else if (value instanceof Boolean || value instanceof Number) {
            sb.append(value);
        } else if (value instanceof Map<?, ?> map) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                appendString(sb, String.valueOf(entry.getKey()));
                sb.append(':');
                appendJson(sb, entry.getValue());
            }
            sb.append('}');
        } else if (value instanceof Iterable<?> iterable) {
            sb.append('[');
            boolean first = true;
            for (Object item : iterable) {
                if (!first) sb.append(',');
                first = false;
                appendJson(sb, item);
            }
            sb.append(']');
        } else if (value instanceof byte[] bytes) {
            sb.append('"');
            for (byte b : bytes) {
                sb.append(String.format("\\u%04x", b & 0xff));
            }
            sb.append('"');
        } else {
            throw new IllegalArgumentException("Unsupported JSON type: " + value.getClass().getName());
        }
    }

    private static void appendString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }
}
