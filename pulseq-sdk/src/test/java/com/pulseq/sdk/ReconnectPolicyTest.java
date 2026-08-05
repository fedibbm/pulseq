package com.pulseq.sdk;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ReconnectPolicyTest {

    @Test
    void backoffDoublesEachAttempt() {
        ReconnectPolicy policy = new ReconnectPolicy(100, 10_000);
        assertEquals(100, policy.nextDelayMillis());
        assertEquals(200, policy.nextDelayMillis());
        assertEquals(400, policy.nextDelayMillis());
        assertEquals(800, policy.nextDelayMillis());
    }

    @Test
    void backoffIsCappedAtMax() {
        ReconnectPolicy policy = new ReconnectPolicy(100, 500);
        assertEquals(100, policy.nextDelayMillis());
        assertEquals(200, policy.nextDelayMillis());
        assertEquals(400, policy.nextDelayMillis());
        assertEquals(500, policy.nextDelayMillis(), "capped at max delay");
        assertEquals(500, policy.nextDelayMillis());
    }

    @Test
    void resetStartsBackoffOver() {
        ReconnectPolicy policy = new ReconnectPolicy(100, 10_000);
        policy.nextDelayMillis();
        policy.nextDelayMillis();
        policy.reset();
        assertEquals(100, policy.nextDelayMillis());
    }

    @Test
    void rejectsInvalidParams() {
        assertThrows(IllegalArgumentException.class, () -> new ReconnectPolicy(0, 10));
        assertThrows(IllegalArgumentException.class, () -> new ReconnectPolicy(10, 5));
    }
}
