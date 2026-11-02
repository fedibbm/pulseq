package com.pulseq.raft;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;

/**
 * A Raft cluster running in one thread over a simulated network. Time advances one tick per
 * {@link #step()}. The network can delay, drop, duplicate and reorder messages, split into
 * partitions, and nodes can crash and restart. Everything random comes from one seed, so a
 * failing run can be replayed exactly.
 *
 * <p>The safety properties from the Raft paper are checked on every step; a violation throws
 * an {@link AssertionError} that names the seed.</p>
 */
final class SimCluster {

    private record InFlight(long deliverAt, long sequence, Envelope envelope) {
    }

    private record Committed(byte[] command, long termWhenFirstApplied) {
    }

    final long seed;
    private final Random random;
    private final RaftConfig config;
    private final List<Integer> ids = new ArrayList<>();
    private final Map<Integer, MemoryRaftStorage> storages = new HashMap<>();
    private final Map<Integer, RaftNode> nodes = new HashMap<>();
    private final Map<Integer, List<String>> applied = new HashMap<>();
    private final Map<Integer, Integer> group = new HashMap<>();
    private final PriorityQueue<InFlight> network = new PriorityQueue<>(
            Comparator.comparingLong(InFlight::deliverAt).thenComparingLong(InFlight::sequence));

    /** Every command any node has applied, by log index. All nodes must agree with it. */
    private final TreeMap<Long, Committed> committed = new TreeMap<>();
    private final Map<Long, Integer> leaderByTerm = new HashMap<>();

    double dropRate;
    double duplicateRate;
    int minDelay = 1;
    int maxDelay = 1;

    private long now;
    private long sequence;
    private int nextCommand;

    SimCluster(int size, long seed) {
        this(size, seed, RaftConfig.defaults());
    }

    SimCluster(int size, long seed, RaftConfig config) {
        this.seed = seed;
        this.random = new Random(seed);
        this.config = config;
        for (int id = 0; id < size; id++) {
            ids.add(id);
            storages.put(id, new MemoryRaftStorage());
            group.put(id, 0);
        }
        ids.forEach(this::start);
    }

    // ------------------------------------------------------------------ faults

    void crash(int id) {
        nodes.remove(id);
    }

    /** Starts a crashed node again over its surviving storage; its state machine starts empty. */
    void restart(int id) {
        if (!nodes.containsKey(id)) {
            start(id);
        }
    }

    private void start(int id) {
        List<String> log = new ArrayList<>();
        applied.put(id, log);
        long[] lastIndex = {0};
        StateMachine stateMachine = (index, command) -> {
            if (index <= lastIndex[0]) {
                fail("node " + id + " applied index " + index + " after " + lastIndex[0]);
            }
            lastIndex[0] = index;
            Committed first = committed.get(index);
            if (first == null) {
                committed.put(index, new Committed(command, storages.get(id).currentTerm()));
            } else if (!Arrays.equals(first.command(), command)) {
                fail("state machine safety: node " + id + " applied " + text(command) + " at index "
                        + index + " but " + text(first.command()) + " was applied there before");
            }
            log.add(text(command));
        };
        nodes.put(id, new RaftNode(id, ids, storages.get(id), stateMachine, config,
                new Random(random.nextLong())));
    }

    /** Splits the cluster: nodes in different groups cannot exchange messages. */
    void partition(List<List<Integer>> groups) {
        for (int g = 0; g < groups.size(); g++) {
            for (int id : groups.get(g)) {
                group.put(id, g);
            }
        }
    }

    void heal() {
        ids.forEach(id -> group.put(id, 0));
    }

    // ------------------------------------------------------------------ running

    void step() {
        now++;
        for (int id : ids) {
            RaftNode node = nodes.get(id);
            if (node != null) {
                node.tick();
                flush(node);
            }
        }
        while (!network.isEmpty() && network.peek().deliverAt() <= now) {
            Envelope envelope = network.poll().envelope();
            RaftNode to = nodes.get(envelope.to());
            if (to != null && group.get(envelope.from()).equals(group.get(envelope.to()))) {
                to.step(envelope);
                flush(to);
            }
        }
        checkInvariants();
    }

    void run(int steps) {
        for (int i = 0; i < steps; i++) {
            step();
        }
    }

    /** Steps until the condition holds; fails if it does not within {@code maxSteps}. */
    void runUntil(String what, int maxSteps, BooleanSupplier condition) {
        for (int i = 0; i < maxSteps; i++) {
            if (condition.getAsBoolean()) {
                return;
            }
            step();
        }
        if (!condition.getAsBoolean()) {
            fail("gave up waiting for: " + what + " (" + maxSteps + " steps)\n" + describe());
        }
    }

    private void flush(RaftNode node) {
        for (Envelope envelope : node.drainOutbox()) {
            if (random.nextDouble() < dropRate) {
                continue;
            }
            schedule(envelope);
            if (random.nextDouble() < duplicateRate) {
                schedule(envelope);
            }
        }
    }

    private void schedule(Envelope envelope) {
        long delay = minDelay + random.nextInt(maxDelay - minDelay + 1);
        network.add(new InFlight(now + delay, sequence++, envelope));
    }

    // ------------------------------------------------------------------ clients

    /** Proposes a fresh, unique command on the given node. Returns the command, or null if refused. */
    String propose(int id) {
        RaftNode node = nodes.get(id);
        if (node == null) {
            return null;
        }
        String command = "c" + nextCommand;
        if (node.propose(command.getBytes(StandardCharsets.UTF_8)) < 0) {
            return null;
        }
        nextCommand++;
        flush(node);
        return command;
    }

    /** Makes a live node start an election immediately. */
    void campaign(int id) {
        RaftNode node = nodes.get(id);
        node.campaign();
        flush(node);
    }

    /** The live leader with the highest term, or null. */
    RaftNode leader() {
        RaftNode best = null;
        for (RaftNode node : nodes.values()) {
            if (node.isLeader() && (best == null || node.currentTerm() > best.currentTerm())) {
                best = node;
            }
        }
        return best;
    }

    RaftNode awaitLeader() {
        runUntil("a leader", 2_000, () -> leader() != null);
        return leader();
    }

    /** Waits until every live node has applied the leader's whole log. */
    void awaitConvergence() {
        runUntil("all live nodes to agree", 5_000, this::converged);
    }

    boolean converged() {
        RaftNode leader = leader();
        if (leader == null || leader.commitIndex() != leader.lastLogIndex()) {
            return false;
        }
        for (RaftNode node : nodes.values()) {
            if (node.lastApplied() != leader.lastLogIndex() || node.currentTerm() != leader.currentTerm()) {
                return false;
            }
        }
        return true;
    }

    RaftNode node(int id) {
        return nodes.get(id);
    }

    boolean isAlive(int id) {
        return nodes.containsKey(id);
    }

    List<Integer> ids() {
        return ids;
    }

    List<String> applied(int id) {
        return applied.get(id);
    }

    /** Every command that some node has applied, in log order. */
    List<String> committedCommands() {
        return committed.values().stream().map(c -> text(c.command())).toList();
    }

    long now() {
        return now;
    }

    // ------------------------------------------------------------------ invariants

    private void checkInvariants() {
        for (RaftNode node : nodes.values()) {
            if (!node.isLeader()) {
                continue;
            }
            Integer known = leaderByTerm.putIfAbsent(node.currentTerm(), node.id());
            if (known == null) {
                checkLeaderCompleteness(node);
            } else if (known != node.id()) {
                fail("election safety: nodes " + known + " and " + node.id()
                        + " are both leaders of term " + node.currentTerm());
            }
        }
        if (now % 25 == 0) {
            checkLogMatching();
        }
    }

    /** A new leader must already hold every entry committed in an earlier term. */
    private void checkLeaderCompleteness(RaftNode leader) {
        MemoryRaftStorage log = storages.get(leader.id());
        committed.forEach((index, entry) -> {
            if (entry.termWhenFirstApplied() >= leader.currentTerm()) {
                return;
            }
            if (index > log.lastIndex() || !Arrays.equals(log.entry(index).command(), entry.command())) {
                fail("leader completeness: node " + leader.id() + " became leader of term "
                        + leader.currentTerm() + " without committed entry " + index + " ("
                        + text(entry.command()) + ")");
            }
        });
    }

    /** If two logs agree on the term at an index, they must be identical up to that index. */
    void checkLogMatching() {
        for (int a : ids) {
            for (int b : ids) {
                if (a >= b) {
                    continue;
                }
                MemoryRaftStorage logA = storages.get(a);
                MemoryRaftStorage logB = storages.get(b);
                long index = Math.min(logA.lastIndex(), logB.lastIndex());
                while (index > 0 && logA.termAt(index) != logB.termAt(index)) {
                    index--;
                }
                for (long i = index; i > 0; i--) {
                    if (!logA.entry(i).equals(logB.entry(i))) {
                        fail("log matching: nodes " + a + " and " + b + " agree at index " + index
                                + " but differ at " + i + ": " + logA.entry(i) + " vs " + logB.entry(i));
                    }
                }
            }
        }
    }

    private void fail(String message) {
        throw new AssertionError("[seed " + seed + ", tick " + now + "] " + message);
    }

    String describe() {
        StringBuilder out = new StringBuilder();
        for (int id : ids) {
            RaftNode node = nodes.get(id);
            MemoryRaftStorage log = storages.get(id);
            out.append("  node ").append(id).append(": ");
            if (node == null) {
                out.append("crashed");
            } else {
                out.append(node.role()).append(" term=").append(node.currentTerm())
                        .append(" commit=").append(node.commitIndex())
                        .append(" applied=").append(node.lastApplied());
            }
            out.append(" lastIndex=").append(log.lastIndex()).append(" group=").append(group.get(id))
                    .append('\n');
        }
        return out.toString();
    }

    private static String text(byte[] command) {
        return new String(command, StandardCharsets.UTF_8);
    }
}
