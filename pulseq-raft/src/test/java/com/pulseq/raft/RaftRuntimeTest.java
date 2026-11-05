package com.pulseq.raft;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** The runtime on real threads and real timers: three nodes in one JVM. */
class RaftRuntimeTest {

    private static final List<Integer> MEMBERS = List.of(0, 1, 2);
    private static final long TICK_MILLIS = 5;

    private final LocalNetwork network = new LocalNetwork();
    private final Map<Integer, MemoryRaftStorage> storages = new HashMap<>();
    private final Map<Integer, RaftRuntime> nodes = new HashMap<>();
    private final Map<Integer, List<String>> applied = new HashMap<>();
    private final List<String> leadershipEvents = new CopyOnWriteArrayList<>();

    @AfterEach
    void stopAll() {
        nodes.values().forEach(RaftRuntime::close);
    }

    @Test
    void commitsCommandsOnEveryNodeInOrder() throws Exception {
        startAll();
        RaftRuntime leader = awaitLeader();

        List<String> sent = new ArrayList<>();
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            sent.add("c" + i);
            futures.add(leader.propose(bytes("c" + i)));
        }
        for (CompletableFuture<Long> future : futures) {
            future.get(5, TimeUnit.SECONDS);
        }
        // Committed means applied on the leader already; followers learn of it a moment later.
        assertEquals(sent, applied.get(leader.id()));
        for (int id : MEMBERS) {
            await("node " + id + " to apply everything", () -> applied.get(id).size() == sent.size());
            assertEquals(sent, applied.get(id));
        }
    }

    @Test
    void followerRejectsCommandsAndNamesTheLeader() {
        startAll();
        RaftRuntime leader = awaitLeader();
        RaftRuntime follower = nodes.get((leader.id() + 1) % 3);
        await("the follower to learn who leads", () -> follower.leaderId() == leader.id());

        ExecutionException error = assertThrows(ExecutionException.class,
                () -> follower.propose(bytes("x")).get(5, TimeUnit.SECONDS));
        NotLeaderException notLeader = assertInstanceOf(NotLeaderException.class, error.getCause());
        assertEquals(leader.id(), notLeader.leaderId());
    }

    @Test
    void anotherNodeTakesOverWhenTheLeaderStops() throws Exception {
        startAll();
        RaftRuntime first = awaitLeader();
        first.propose(bytes("before")).get(5, TimeUnit.SECONDS);

        stop(first.id());
        RaftRuntime second = awaitLeader();
        assertNotEquals(first.id(), second.id());
        second.propose(bytes("after")).get(5, TimeUnit.SECONDS);
        assertEquals(List.of("before", "after"), applied.get(second.id()));

        start(first.id());
        await("the old leader to catch up", () -> applied.get(first.id()).size() == 2);
        assertEquals(List.of("before", "after"), applied.get(first.id()));
    }

    @Test
    void commandOnACutOffLeaderFailsOnceItIsReplaced() throws Exception {
        startAll();
        RaftRuntime old = awaitLeader();
        old.propose(bytes("kept")).get(5, TimeUnit.SECONDS);

        network.disconnect(old.id());
        CompletableFuture<Long> stranded = old.propose(bytes("stranded"));
        await("the other two to elect a leader",
                () -> nodes.values().stream().anyMatch(n -> n.id() != old.id() && n.isLeaderReady()));
        assertTrue(!stranded.isDone(), "undecided while the old leader is cut off");

        network.reconnect(old.id());
        ExecutionException error = assertThrows(ExecutionException.class,
                () -> stranded.get(5, TimeUnit.SECONDS));
        assertInstanceOf(NotLeaderException.class, error.getCause());
        for (int id : MEMBERS) {
            assertEquals(List.of("kept"), applied.get(id), "node " + id);
        }
    }

    @Test
    void reportsLeadershipOnlyAfterCatchingUp() throws Exception {
        startAll();
        RaftRuntime first = awaitLeader();
        first.propose(bytes("a")).get(5, TimeUnit.SECONDS);
        assertEquals(List.of("ready " + first.id()), leadershipEvents);

        stop(first.id());
        RaftRuntime second = awaitLeader();
        assertEquals(List.of("ready " + first.id(), "lost " + first.id(), "ready " + second.id()),
                leadershipEvents);
        // The listener fired with "a" already applied on the new leader.
        assertEquals(List.of("a"), applied.get(second.id()));
    }

    // ------------------------------------------------------------------ helpers

    private void startAll() {
        MEMBERS.forEach(id -> storages.put(id, new MemoryRaftStorage()));
        MEMBERS.forEach(this::start);
    }

    private void start(int id) {
        List<String> log = new CopyOnWriteArrayList<>();
        applied.put(id, log);
        LeadershipListener listener = new LeadershipListener() {
            @Override
            public void onLeaderReady(long term) {
                leadershipEvents.add("ready " + id);
            }

            @Override
            public void onLeadershipLost() {
                leadershipEvents.add("lost " + id);
            }
        };
        RaftRuntime node = new RaftRuntime(id, MEMBERS, storages.get(id),
                (index, command) -> log.add(new String(command, StandardCharsets.UTF_8)),
                RaftConfig.defaults(), network.transport(), listener, TICK_MILLIS);
        nodes.put(id, node);
        network.register(node);
        node.start();
    }

    private void stop(int id) {
        network.unregister(id);
        nodes.remove(id).close();
    }

    private RaftRuntime awaitLeader() {
        await("a ready leader", () -> nodes.values().stream().anyMatch(RaftRuntime::isLeaderReady));
        return nodes.values().stream().filter(RaftRuntime::isLeaderReady).findFirst().orElseThrow();
    }

    private void await(String what, BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            for (RaftRuntime node : nodes.values()) {
                if (node.failure() != null) {
                    fail("node " + node.id() + " failed", node.failure());
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

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
