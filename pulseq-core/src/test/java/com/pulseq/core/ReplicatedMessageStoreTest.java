package com.pulseq.core;

import com.pulseq.raft.LeadershipListener;
import com.pulseq.raft.LocalNetwork;
import com.pulseq.raft.MemoryRaftStorage;
import com.pulseq.raft.NotLeaderException;
import com.pulseq.raft.RaftConfig;
import com.pulseq.raft.RaftRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Three brokers' stores kept in step by Raft, in one JVM. */
class ReplicatedMessageStoreTest {

    private static final List<Integer> MEMBERS = List.of(0, 1, 2);
    private static final LeadershipListener NO_LISTENER = new LeadershipListener() {
        @Override
        public void onLeaderReady(long term) {
        }

        @Override
        public void onLeadershipLost() {
        }
    };

    private final LocalNetwork network = new LocalNetwork();
    private final Map<Integer, InMemoryMessageStore> locals = new HashMap<>();
    private final Map<Integer, ReplicatedMessageStore> stores = new HashMap<>();
    private final Map<Integer, RaftRuntime> nodes = new HashMap<>();
    /** How many messages each broker's own store removed in sweeps. */
    private final Map<Integer, AtomicInteger> swept = new HashMap<>();

    @BeforeEach
    void startCluster() {
        for (int id : MEMBERS) {
            AtomicInteger sweptHere = new AtomicInteger();
            swept.put(id, sweptHere);
            InMemoryMessageStore local = new InMemoryMessageStore() {
                @Override
                public int sweepCompleted(long cutoffMillis) {
                    int removed = super.sweepCompleted(cutoffMillis);
                    sweptHere.addAndGet(removed);
                    return removed;
                }
            };
            ReplicatedMessageStore store = new ReplicatedMessageStore(local, 2_000);
            RaftRuntime node = new RaftRuntime(id, MEMBERS, new MemoryRaftStorage(), store,
                    RaftConfig.defaults(), network.transport(), NO_LISTENER, 5);
            store.attach(node);
            locals.put(id, local);
            stores.put(id, store);
            nodes.put(id, node);
            network.register(node);
            node.start();
        }
    }

    @AfterEach
    void stopCluster() {
        nodes.values().forEach(RaftRuntime::close);
    }

    @Test
    void newLeaderRecoversExactlyTheUnfinishedMessages() {
        int first = awaitLeader();
        QueueManager broker = new QueueManager(stores.get(first));
        for (int i = 0; i < 20; i++) {
            assertTrue(broker.publish("orders", new Message(id(i), "orders", new byte[] {(byte) i})));
        }
        MessageQueue queue = broker.getQueue("orders");
        for (int i = 0; i < 10; i++) {
            assertEquals(id(i), queue.dequeue().getId());
        }
        for (int i = 0; i < 6; i++) {
            assertTrue(queue.ack(id(i)));          // m00..m05 finished
        }
        assertTrue(queue.nack(id(6), Reason.REJECTED)); // m06 dead-lettered
        // m07..m09 are still being processed; m10..m19 were never delivered.

        for (int id : MEMBERS) {
            await("broker " + id + " to hold every change", () ->
                    locals.get(id).loadAllAvailable().size() == 13
                            && locals.get(id).loadDeadLettered().size() == 1);
        }

        stop(first);
        int second = awaitLeader();
        assertNotEquals(first, second);
        QueueManager successor = new QueueManager(stores.get(second));
        successor.recover();

        MessageQueue recovered = successor.getQueue("orders");
        List<String> redelivered = new ArrayList<>();
        while (recovered.size() > 0) {
            Message message = recovered.dequeue();
            redelivered.add(message.getId());
            assertEquals(1, message.getPayload().length, "payload survived");
        }
        List<String> expected = new ArrayList<>();
        for (int i = 7; i < 20; i++) {
            expected.add(id(i));
        }
        assertEquals(expected, redelivered, "in-flight and undelivered messages, nothing acknowledged");
        assertEquals(1, recovered.getDeadLetterQueue().size());
    }

    @Test
    void followerRefusesWritesAndThePublishCanBeRetried() {
        int leader = awaitLeader();
        int follower = (leader + 1) % 3;
        await("the follower to learn who leads", () -> nodes.get(follower).leaderId() == leader);
        QueueManager onFollower = new QueueManager(stores.get(follower));

        Message message = new Message("m1", "orders", new byte[0]);
        NotLeaderException refused = assertThrows(NotLeaderException.class,
                () -> onFollower.publish("orders", message));
        assertEquals(leader, refused.leaderId());
        assertEquals(0, onFollower.getQueue("orders").size());

        // The refused publish left no trace, so the same id is accepted by the leader...
        assertTrue(new QueueManager(stores.get(leader)).publish("orders", message));
        // ...and would be accepted here too once this broker leads: the id was forgotten.
        assertThrows(NotLeaderException.class, () -> onFollower.publish("orders", message));
    }

    @Test
    void writeOnACutOffLeaderTimesOutInsteadOfHanging() {
        int leader = awaitLeader();
        ReplicatedMessageStore impatient = new ReplicatedMessageStore(new InMemoryMessageStore(), 200);
        impatient.attach(nodes.get(leader));
        network.disconnect(leader);

        assertThrows(ReplicationException.class,
                () -> impatient.markAcknowledged("anything"));
    }

    @Test
    void sweepRemovesFinishedMessagesOnEveryBroker() {
        int leader = awaitLeader();
        QueueManager broker = new QueueManager(stores.get(leader));
        for (int i = 0; i < 5; i++) {
            broker.publish("orders", new Message(id(i), "orders", new byte[0]));
        }
        MessageQueue queue = broker.getQueue("orders");
        for (int i = 0; i < 3; i++) {
            queue.ack(queue.dequeue().getId());
        }

        int removed = stores.get(leader).sweepCompleted(System.currentTimeMillis() + 1_000);
        assertEquals(3, removed);
        for (int id : MEMBERS) {
            await("broker " + id + " to sweep", () -> swept.get(id).get() == 3);
            assertEquals(2, locals.get(id).loadAllAvailable().size(), "unfinished messages are kept");
        }
    }

    // ------------------------------------------------------------------ helpers

    private static String id(int i) {
        return String.format("m%02d", i);
    }

    private void stop(int id) {
        network.unregister(id);
        nodes.remove(id).close();
    }

    private int awaitLeader() {
        await("a ready leader", () -> nodes.values().stream().anyMatch(RaftRuntime::isLeaderReady));
        return nodes.values().stream().filter(RaftRuntime::isLeaderReady).findFirst().orElseThrow().id();
    }

    private void await(String what, BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            for (RaftRuntime node : nodes.values()) {
                if (node.failure() != null) {
                    fail("broker " + node.id() + " failed", node.failure());
                }
            }
            if (System.nanoTime() > deadline) {
                fail("gave up waiting for " + what);
            }
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted");
            }
        }
    }
}
