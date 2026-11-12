package com.pulseq.core;

import com.pulseq.raft.FileRaftStorage;
import com.pulseq.raft.RaftConfig;
import com.pulseq.raft.TcpTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Three brokers talking over real TCP sockets with their Raft logs on disk, as separate
 * processes would. Each broker's message store is in memory, so after a restart it is rebuilt
 * entirely from the log on disk.
 */
class ClusteredBrokerTcpTest {

    private static final List<Integer> MEMBERS = List.of(0, 1, 2);
    private static final String TOPIC = "orders";

    @TempDir
    Path directory;

    private final Map<Integer, InetSocketAddress> addresses = new HashMap<>();
    private final Map<Integer, ClusteredBroker> brokers = new ConcurrentHashMap<>();
    private final Map<Integer, TcpTransport> transports = new ConcurrentHashMap<>();
    private final Map<Integer, FileRaftStorage> logs = new ConcurrentHashMap<>();
    private final Map<Integer, InMemoryMessageStore> stores = new ConcurrentHashMap<>();
    private final Set<String> received = new ConcurrentSkipListSet<>();
    private volatile boolean consuming = true;

    @AfterEach
    void stopCluster() {
        List.copyOf(brokers.keySet()).forEach(this::stop);
    }

    @Test
    void leaderLossOverTcpLosesNothing() throws IOException {
        startAll();
        ClusteredBroker first = servingBroker();
        for (int i = 0; i < 50; i++) {
            assertTrue(first.publish(TOPIC, message(i)));
        }
        stop(first.id());

        ClusteredBroker second = servingBroker();
        assertNotEquals(first.id(), second.id());
        for (int i = 50; i < 100; i++) {
            assertTrue(second.publish(TOPIC, message(i)));
        }
        await("all 100 messages to reach a consumer", () -> received.size() == 100);

        // The stopped broker returns with an empty message store and only its log on disk.
        start(first.id());
        await("the returning broker to replay and catch up",
                () -> brokers.get(first.id()).raft().commitIndex() == second.raft().commitIndex()
                        && second.raft().commitIndex() > 0);
        await("its rebuilt store to show nothing unfinished",
                () -> stores.get(first.id()).loadAllAvailable().isEmpty());
    }

    @Test
    void wholeClusterRestartKeepsUnfinishedMessages() throws IOException {
        consuming = false; // nothing is acknowledged before the restart
        startAll();
        ClusteredBroker leader = servingBroker();
        Set<String> published = new TreeSet<>();
        for (int i = 0; i < 30; i++) {
            assertTrue(leader.publish(TOPIC, message(i)));
            published.add(message(i).getId());
        }

        List.copyOf(brokers.keySet()).forEach(this::stop);   // every broker goes down
        assertTrue(received.isEmpty());

        consuming = true;
        MEMBERS.forEach(this::start);                        // and comes back from disk
        servingBroker();
        await("every message published before the restart to be delivered",
                () -> received.containsAll(published));
        assertEquals(published, new TreeSet<>(received));
    }

    // ------------------------------------------------------------------ helpers

    private void startAll() throws IOException {
        for (int id : MEMBERS) {
            try (ServerSocket probe = new ServerSocket(0)) {
                addresses.put(id, new InetSocketAddress("127.0.0.1", probe.getLocalPort()));
            }
        }
        MEMBERS.forEach(this::start);
    }

    private void start(int id) {
        InMemoryMessageStore store = new InMemoryMessageStore();
        FileRaftStorage log = new FileRaftStorage(directory.resolve("broker-" + id));
        TcpTransport transport = new TcpTransport(id, addresses);
        BrokerConfig config = new BrokerConfig(1_000, 2_000, 50, 5_000, 3, 4, 20);
        ClusteredBroker broker = new ClusteredBroker(id, MEMBERS, store, log, transport, config,
                new ClusterConfig(RaftConfig.defaults(), 10, 2_000));
        broker.subscribe(TOPIC, "workers", message -> {
            if (!consuming) {
                return; // left unacknowledged
            }
            received.add(message.getId());
            try {
                broker.ack(TOPIC, message.getId());
            } catch (RuntimeException brokerChanged) {
                // Delivered again later.
            }
        });
        stores.put(id, store);
        logs.put(id, log);
        transports.put(id, transport);
        brokers.put(id, broker);
        transport.start(broker.raft()::receive);
        broker.start();
    }

    private void stop(int id) {
        brokers.remove(id).close();
        transports.remove(id).close();
        logs.remove(id).close();
    }

    private static Message message(int i) {
        String id = String.format("m%03d", i);
        return new Message(id, TOPIC, id.getBytes());
    }

    private ClusteredBroker servingBroker() {
        await("a serving broker", () -> brokers.values().stream().anyMatch(ClusteredBroker::isServing));
        return brokers.values().stream().filter(ClusteredBroker::isServing).findFirst().orElseThrow();
    }

    private void await(String what, BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!condition.getAsBoolean()) {
            for (ClusteredBroker broker : brokers.values()) {
                if (broker.raft().failure() != null) {
                    fail("broker " + broker.id() + " failed", broker.raft().failure());
                }
            }
            if (System.nanoTime() > deadline) {
                fail("gave up waiting for " + what);
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted");
            }
        }
    }
}
