package com.pulseq.core;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test for {@link PostgresMessageStore} against a live PostgreSQL instance.
 *
 * <p>The test self-skips when no database is reachable (e.g. plain {@code mvn test}
 * without Docker), and runs against the same schema/database used by docker-compose.</p>
 */
class PostgresMessageStoreIT {

    private static final String URL = "jdbc:postgresql://localhost:5432/pulseq";
    private static final String USER = "pulseq";
    private static final String PASSWORD = "pulseq";

    private static boolean databaseAvailable;

    @BeforeAll
    static void connect() {
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.execute(new String(PostgresMessageStoreIT.class.getResourceAsStream("/schema.sql").readAllBytes()));
            stmt.execute("DELETE FROM messages WHERE id LIKE 'it-%'");
            databaseAvailable = true;
        } catch (Exception e) {
            databaseAvailable = false;
        }
    }

    @Test
    void saveLoadAndAckRoundTrip() {
        assumeTrue(databaseAvailable, "PostgreSQL not reachable; skipping integration test");
        PostgresMessageStore store = new PostgresMessageStore(URL, USER, PASSWORD);

        Message m = new Message("it-1", "orders", Payloads.toBytes("{\"qty\":3}"), 5, 1000);
        store.save(m);

        List<Message> loaded = store.loadAllAvailable();
        Message found = loaded.stream().filter(x -> x.getId().equals("it-1")).findFirst().orElseThrow();
        assertEquals("orders", found.getTopic());
        assertEquals("{\"qty\":3}", found.getPayloadAsString());
        assertEquals(5, found.getMaxRetries());
        assertEquals(1000, found.getTtlMillis());
        assertEquals(MessageStatus.AVAILABLE, found.getStatus());

        store.markInFlight("it-1", 123456L);
        Message inFlight = store.loadAllAvailable().stream()
                .filter(x -> x.getId().equals("it-1")).findFirst().orElseThrow();
        assertEquals(MessageStatus.IN_FLIGHT, inFlight.getStatus());
        assertEquals(123456L, inFlight.getVisibilityExpiresAt());

        store.markAcknowledged("it-1");
        assertTrue(store.loadAllAvailable().stream().noneMatch(x -> x.getId().equals("it-1")),
                "acknowledged message must not be reloaded");
    }

    @Test
    void deadLetteredMessagesAreNotReloaded() {
        assumeTrue(databaseAvailable, "PostgreSQL not reachable; skipping integration test");
        PostgresMessageStore store = new PostgresMessageStore(URL, USER, PASSWORD);

        Message m = new Message("it-2", "orders", Payloads.toBytes("boom"));
        store.save(m);
        store.markDeadLettered("it-2");

        assertTrue(store.loadAllAvailable().stream().noneMatch(x -> x.getId().equals("it-2")),
                "dead-lettered message must not be reloaded as available");
    }

    @Test
    void saveIsIdempotentPerMessageId() {
        assumeTrue(databaseAvailable, "PostgreSQL not reachable; skipping integration test");
        PostgresMessageStore store = new PostgresMessageStore(URL, USER, PASSWORD);

        store.save(new Message("it-3", "orders", Payloads.toBytes("first")));
        store.save(new Message("it-3", "orders", Payloads.toBytes("first")));

        long count = store.loadAllAvailable().stream().filter(x -> x.getId().equals("it-3")).count();
        assertEquals(1, count, "upsert must not create duplicates");
    }

    @Test
    void loadDeadLetteredReturnsOnlyDeadLetteredOldestFirst() {
        assumeTrue(databaseAvailable, "PostgreSQL not reachable; skipping integration test");
        PostgresMessageStore store = new PostgresMessageStore(URL, USER, PASSWORD);

        Message dead = new Message("it-dlq-1", "orders", Payloads.toBytes("boom"), 5, 0);
        store.save(dead);
        store.markDeadLettered(dead.getId());
        Message live = new Message("it-live-1", "orders", Payloads.toBytes("ok"));
        store.save(live);

        List<Message> deadLettered = store.loadDeadLettered();

        assertTrue(deadLettered.stream().anyMatch(x -> x.getId().equals("it-dlq-1")),
                "dead-lettered message must be reloaded for DLQ rebuild");
        assertTrue(deadLettered.stream().noneMatch(x -> x.getId().equals("it-live-1")),
                "available message must not be returned");
        Message restored = deadLettered.stream().filter(x -> x.getId().equals("it-dlq-1"))
                .findFirst().orElseThrow();
        assertEquals("boom", restored.getPayloadAsString());
        assertEquals(MessageStatus.DEAD_LETTERED, restored.getStatus());
        assertEquals(5, restored.getMaxRetries());
    }

    @Test
    void sweepCompletedRemovesOldTerminalRowsButKeepsRecoverable() {
        assumeTrue(databaseAvailable, "PostgreSQL not reachable; skipping integration test");
        PostgresMessageStore store = new PostgresMessageStore(URL, USER, PASSWORD);

        long now = System.currentTimeMillis();
        Message oldAcked = new Message("it-sweep-1", "orders", Payloads.toBytes("old"),
                now - 3_600_000, 0, 0, 3, MessageStatus.ACKNOWLEDGED, 0);
        Message oldDead = new Message("it-sweep-2", "orders", Payloads.toBytes("old"),
                now - 3_600_000, 0, 0, 3, MessageStatus.DEAD_LETTERED, 0);
        Message freshAvailable = new Message("it-sweep-3", "orders", Payloads.toBytes("new"));
        store.save(oldAcked);
        store.save(oldDead);
        store.save(freshAvailable);
        store.markAcknowledged(oldAcked.getId());
        store.markDeadLettered(oldDead.getId());

        int removed = store.sweepCompleted(now - 1_000);

        assertTrue(removed >= 2, "old terminal rows are swept, removed=" + removed);
        assertTrue(store.loadDeadLettered().stream().noneMatch(x -> x.getId().equals("it-sweep-2")),
                "swept dead-lettered message must be gone");
        assertTrue(store.loadAllAvailable().stream().anyMatch(x -> x.getId().equals("it-sweep-3")),
                "fresh available message must survive the sweep");
    }
}
