package com.pulseq.raft;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs a {@link RaftNode} on one dedicated thread: ticks it on a timer, feeds it incoming
 * messages, sends its outgoing ones, and tells callers when their commands are committed.
 *
 * <p>The state machine and the {@link LeadershipListener} are called on that thread, so they
 * never run concurrently with each other, but they delay the node for as long as they take.</p>
 */
public final class RaftRuntime implements AutoCloseable {

    private record Pending(long term, CompletableFuture<Long> future) {
    }

    private final int id;
    private final RaftNode node;
    private final RaftStorage storage;
    private final Transport transport;
    private final LeadershipListener listener;
    private final long tickMillis;
    private final ScheduledExecutorService loop;

    // Touched only by the loop thread.
    private final TreeMap<Long, Pending> pending = new TreeMap<>();
    private boolean leaderReady;

    // Snapshots for other threads.
    private volatile boolean leader;
    private volatile boolean ready;
    private volatile int leaderId = RaftNode.NO_LEADER;
    private volatile long term;
    private volatile long commitIndex;
    private volatile Throwable failure;

    public RaftRuntime(int id, List<Integer> members, RaftStorage storage, StateMachine stateMachine,
                       RaftConfig config, Transport transport, LeadershipListener listener,
                       long tickMillis) {
        this.id = id;
        this.storage = storage;
        this.transport = transport;
        this.listener = listener;
        this.tickMillis = tickMillis;
        this.node = new RaftNode(id, members, storage, stateMachine, config,
                new Random(System.nanoTime() ^ (id * 0x9E3779B97F4A7C15L)));
        this.loop = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "pulseq-raft-" + id);
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() {
        // Fixed delay, not fixed rate: after a stall (a slow disk sync, a long pause) a fixed
        // rate would fire every missed tick at once, ahead of the messages that queued up
        // meanwhile, and the node would call an election it has no reason to call.
        loop.scheduleWithFixedDelay(() -> run(node::tick), tickMillis, tickMillis, TimeUnit.MILLISECONDS);
    }

    /** Hands a message from another node to this one. Safe to call from any thread. */
    public void receive(Envelope envelope) {
        try {
            loop.execute(() -> run(() -> node.step(envelope)));
        } catch (RejectedExecutionException closed) {
            // A stopped node drops messages, like a crashed process.
        }
    }

    /**
     * Submits a command. The future completes with the command's log index once a majority
     * holds it and this node has applied it. It fails with {@link NotLeaderException} when
     * this node is not the leader or loses leadership before the command commits; in that
     * case the command had no effect. While a leader is cut off from the majority the future
     * stays open, so callers should wait with a timeout.
     */
    public CompletableFuture<Long> propose(byte[] command) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            loop.execute(() -> run(() -> {
                long index = node.propose(command);
                if (index < 0) {
                    future.completeExceptionally(new NotLeaderException(node.leaderId()));
                } else {
                    pending.put(index, new Pending(node.currentTerm(), future));
                }
            }));
        } catch (RejectedExecutionException closed) {
            future.completeExceptionally(new IllegalStateException("node " + id + " is stopped"));
        }
        return future;
    }

    /** One loop iteration: do the work, then deal with everything it caused. */
    private void run(Runnable work) {
        if (failure != null) {
            return;
        }
        try {
            work.run();
            // Replies and commit notices promise that appended entries exist, so the entries
            // must be on disk before any of them leaves this node.
            storage.flush();
            for (Envelope envelope : node.drainOutbox()) {
                transport.send(envelope);
            }
            settlePending();
            publishState();
            notifyListener();
        } catch (Throwable t) {
            // A node whose invariants broke must stop rather than keep voting and replicating.
            failure = t;
            leader = false;
            ready = false;
            failPending(new IllegalStateException("node " + id + " failed", t));
            loop.shutdown();
        }
    }

    private void settlePending() {
        Iterator<Map.Entry<Long, Pending>> entries = pending.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<Long, Pending> entry = entries.next();
            long index = entry.getKey();
            Pending proposal = entry.getValue();
            boolean stillInLog = index <= storage.lastIndex() && storage.termAt(index) == proposal.term();
            if (!stillInLog) {
                // Another leader replaced the entry: it will never commit.
                proposal.future().completeExceptionally(new NotLeaderException(node.leaderId()));
                entries.remove();
            } else if (index <= node.lastApplied()) {
                proposal.future().complete(index);
                entries.remove();
            } else {
                break; // later entries cannot be decided before this one
            }
        }
    }

    private void publishState() {
        leader = node.isLeader();
        leaderId = node.leaderId();
        term = node.currentTerm();
        commitIndex = node.commitIndex();
    }

    private void notifyListener() {
        // A leader has caught up once an entry of its own term is committed, because that
        // commits everything before it too.
        boolean nowReady = node.isLeader()
                && storage.termAt(node.commitIndex()) == node.currentTerm()
                && node.lastApplied() == node.commitIndex();
        if (nowReady != leaderReady) {
            leaderReady = nowReady;
            ready = nowReady;
            if (nowReady) {
                listener.onLeaderReady(node.currentTerm());
            } else {
                listener.onLeadershipLost();
            }
        }
    }

    private void failPending(RuntimeException cause) {
        pending.values().forEach(p -> p.future().completeExceptionally(cause));
        pending.clear();
    }

    /** Stops the node, like a crash: its storage keeps whatever was written. */
    @Override
    public void close() {
        try {
            loop.execute(() -> {
                failPending(new IllegalStateException("node " + id + " is stopped"));
                if (leaderReady) {
                    leaderReady = false;
                    listener.onLeadershipLost();
                }
                leader = false;
                ready = false;
            });
        } catch (RejectedExecutionException alreadyClosed) {
            return;
        }
        loop.shutdown();
        try {
            loop.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public int id() {
        return id;
    }

    /** True while this node believes it is the leader. A cut-off leader can believe it wrongly. */
    public boolean isLeader() {
        return leader;
    }

    /** True once this node leads and its state machine has caught up; see {@link LeadershipListener}. */
    public boolean isLeaderReady() {
        return ready;
    }

    public int leaderId() {
        return leaderId;
    }

    public long term() {
        return term;
    }

    public long commitIndex() {
        return commitIndex;
    }

    /** The error that stopped this node, or null. */
    public Throwable failure() {
        return failure;
    }
}
