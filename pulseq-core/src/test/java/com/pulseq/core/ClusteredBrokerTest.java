package com.pulseq.core;

import com.pulseq.raft.LocalNetwork;
import com.pulseq.raft.MemoryRaftStorage;
import com.pulseq.raft.NotLeaderException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Three whole brokers in one JVM, with producers and consumers, through leader failures. */
class ClusteredBrokerTest {

    private static final List<Integer> MEMBERS = List.of(0, 1, 2);
    private static final String TOPIC = "orders";

    private final LocalNetwork network = new LocalNetwork();
    private final Map<Integer, InMemoryMessageStore> stores = new HashMap<>();
    private final Map<Integer, MemoryRaftStorage> logs = new HashMap<>();
    private final Map<Integer, ClusteredBroker> brokers = new ConcurrentHashMap<>();

    /** Ids the consumers received, and how many deliveries in total (duplicates included). */
    private final Set<String> received = new ConcurrentSkipListSet<>();
    private final AtomicInteger deliveries = new AtomicInteger();

    @BeforeEach
    void startCluster() {
        for (int id : MEMBERS) {
            stores.put(id, new InMemoryMessageStore());
            logs.put(id, new MemoryRaftStorage());
            start(id);
        }
    }

    @AfterEach
    void stopCluster() {
        brokers.values().forEach(ClusteredBroker::close);
    }

    @Test
    void noAcceptedMessageIsLostWhenTheLeaderDiesMidStream() throws Exception {
        int total = 300;
        Set<String> accepted = new ConcurrentSkipListSet<>();
        Thread producer = new Thread(() -> {
            for (int i = 0; i < total; i++) {
                String id = String.format("m%03d", i);
                publishUntilAccepted(id);
                accepted.add(id);
            }
        }, "producer");
        producer.start();

        await("the first 100 messages", () -> accepted.size() >= 100);
        int first = servingBroker().id();
        stop(first);                                   // the leader dies while messages flow

        producer.join(TimeUnit.SECONDS.toMillis(30));
        assertFalse(producer.isAlive(), "producer finished");
        assertEquals(total, accepted.size());
        assertNotEquals(first, servingBroker().id());

        await("every accepted message to reach a consumer", () -> received.containsAll(accepted));
        // At-least-once: messages in flight at the failure may arrive twice, never zero times.
        assertTrue(deliveries.get() >= total);

        // Once consumers have acknowledged everything, no broker holds unfinished work.
        for (int id : brokers.keySet()) {
            await("broker " + id + " to have nothing unfinished",
                    () -> stores.get(id).loadAllAvailable().isEmpty());
        }
    }

    @Test
    void stoppedBrokerComesBackAsAFollowerAndCatchesUp() {
        ClusteredBroker leader = servingBroker();
        int stopped = leader.id();
        assertTrue(leader.publish(TOPIC, new Message("before", TOPIC, new byte[0])));
        stop(stopped);

        ClusteredBroker successor = servingBroker();
        // No consumer yet on purpose: these stay unfinished so the catch-up is observable.
        assertTrue(successor.publish("audit", new Message("a1", "audit", new byte[0])));
        assertTrue(successor.publish("audit", new Message("a2", "audit", new byte[0])));

        start(stopped);
        await("the returning broker to catch up", () -> unfinishedIds(stopped).containsAll(Set.of("a1", "a2")));
        assertFalse(brokers.get(stopped).isServing());
        NotLeaderException refused = assertThrows(NotLeaderException.class,
                () -> brokers.get(stopped).publish(TOPIC, new Message("x", TOPIC, new byte[0])));
        assertEquals(successor.id(), refused.leaderId());
    }

    @Test
    void cutOffLeaderStopsServingOnceItHearsOfItsReplacement() {
        ClusteredBroker old = servingBroker();
        network.disconnect(old.id());
        await("another broker to take over",
                () -> brokers.values().stream().anyMatch(b -> b.id() != old.id() && b.isServing()));
        assertTrue(old.isServing(), "a cut-off leader cannot know it was replaced");
        assertThrows(ReplicationException.class,
                () -> old.publish(TOPIC, new Message("stranded", TOPIC, new byte[0])));

        network.reconnect(old.id());
        await("the old leader to step aside", () -> !old.isServing());
        await("exactly one serving broker",
                () -> brokers.values().stream().filter(ClusteredBroker::isServing).count() == 1);
        for (int id : MEMBERS) {
            assertFalse(unfinishedIds(id).contains("stranded"), "broker " + id);
        }
    }

    @Test
    void repeatedLeaderChangesLoseNothing() throws Exception {
        int total = 200;
        Set<String> accepted = new ConcurrentSkipListSet<>();
        Thread producer = new Thread(() -> {
            for (int i = 0; i < total; i++) {
                String id = String.format("m%03d", i);
                publishUntilAccepted(id);
                accepted.add(id);
            }
        }, "producer");
        producer.start();

        // Cut the current leader off six times while messages flow, so leadership moves around
        // and brokers that led before lead again.
        for (int round = 0; round < 6; round++) {
            int leader = servingBroker().id();
            network.disconnect(leader);
            await("another broker to take over",
                    () -> brokers.values().stream().anyMatch(b -> b.id() != leader && b.isServing()));
            network.reconnect(leader);
            await("the cut-off broker to step aside", () -> !brokers.get(leader).isServing());
        }

        producer.join(TimeUnit.SECONDS.toMillis(60));
        assertFalse(producer.isAlive(), "producer finished");
        await("every accepted message to reach a consumer", () -> received.containsAll(accepted));
        for (int id : MEMBERS) {
            await("broker " + id + " to have nothing unfinished",
                    () -> stores.get(id).loadAllAvailable().isEmpty());
        }
    }

    // ------------------------------------------------------------------ helpers

    private void start(int id) {
        BrokerConfig config = new BrokerConfig(1_000, 2_000, 50, 5_000, 3, 4, 20);
        ClusteredBroker broker = new ClusteredBroker(id, MEMBERS, stores.get(id), logs.get(id),
                network.transport(), config, new ClusterConfig(com.pulseq.raft.RaftConfig.defaults(), 5, 1_000));
        // A consumer group with one member on each broker: only the leader's member is fed.
        broker.subscribe(TOPIC, "workers", message -> {
            received.add(message.getId());
            deliveries.incrementAndGet();
            try {
                broker.ack(TOPIC, message.getId());
            } catch (RuntimeException brokerChanged) {
                // Not acknowledged: the message is delivered again, here or by the next leader.
            }
        });
        brokers.put(id, broker);
        network.register(broker.raft());
        broker.start();
    }

    private void stop(int id) {
        network.unregister(id);
        brokers.remove(id).close();
    }

    private void publishUntilAccepted(String id) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline) {
            for (ClusteredBroker broker : new ArrayList<>(brokers.values())) {
                try {
                    broker.publish(TOPIC, new Message(id, TOPIC, id.getBytes()));
                    return; // stored by a majority (or already stored: a duplicate id)
                } catch (RuntimeException notNow) {
                    // Not the leader, or the leader just died: try the next broker.
                }
            }
            pause();
        }
        throw new AssertionError("could not publish " + id);
    }

    private ClusteredBroker servingBroker() {
        await("a serving broker", () -> brokers.values().stream().anyMatch(ClusteredBroker::isServing));
        return brokers.values().stream().filter(ClusteredBroker::isServing).findFirst().orElseThrow();
    }

    private Set<String> unfinishedIds(int id) {
        Set<String> ids = new TreeSet<>();
        stores.get(id).loadAllAvailable().forEach(message -> ids.add(message.getId()));
        return ids;
    }

    private void await(String what, BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean()) {
            for (ClusteredBroker broker : brokers.values()) {
                if (broker.raft().failure() != null) {
                    fail("broker " + broker.id() + " failed", broker.raft().failure());
                }
            }
            if (System.nanoTime() > deadline) {
                fail("gave up waiting for " + what);
            }
            pause();
        }
    }

    private static void pause() {
        try {
            Thread.sleep(2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted");
        }
    }
}
