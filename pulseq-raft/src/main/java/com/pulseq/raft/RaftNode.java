package com.pulseq.raft;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * One member of a Raft cluster: leader election and log replication.
 *
 * <p>The node has no threads, clock or network of its own. Whoever owns it calls
 * {@link #tick()} at a steady rate, passes incoming messages to {@link #step(Envelope)}, and
 * sends whatever {@link #drainOutbox()} returns. All calls must come from one thread. This
 * keeps the algorithm deterministic, so a simulation can replay any failure from a seed.</p>
 *
 * <p>Not implemented: membership changes, log compaction, and linearizable reads.</p>
 */
public final class RaftNode {

    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    public static final int NO_LEADER = -1;

    private final int id;
    private final List<Integer> peers;
    private final RaftStorage storage;
    private final StateMachine stateMachine;
    private final RaftConfig config;
    private final Random random;

    // Volatile state: lost on a crash and rebuilt from the log and from the leader.
    private Role role = Role.FOLLOWER;
    private int leaderId = NO_LEADER;
    private long commitIndex;
    private long lastApplied;
    private int electionElapsed;
    private int electionTimeout;
    private int heartbeatElapsed;
    private final Set<Integer> votes = new HashSet<>();
    private final Map<Integer, Long> nextIndex = new HashMap<>();
    private final Map<Integer, Long> matchIndex = new HashMap<>();
    private final List<Envelope> outbox = new ArrayList<>();

    /**
     * @param id      this node's id
     * @param members ids of every cluster member, including this node
     * @param storage durable state; pass the same storage again to restart a crashed node
     */
    public RaftNode(int id, List<Integer> members, RaftStorage storage, StateMachine stateMachine,
                    RaftConfig config, Random random) {
        if (!members.contains(id)) {
            throw new IllegalArgumentException("node " + id + " is not in " + members);
        }
        this.id = id;
        this.peers = members.stream().filter(m -> m != id).toList();
        this.storage = storage;
        this.stateMachine = stateMachine;
        this.config = config;
        this.random = random;
        resetElectionTimer();
    }

    // ------------------------------------------------------------------ driving the node

    /** Advances the node's timers by one tick. */
    public void tick() {
        if (role == Role.LEADER) {
            if (++heartbeatElapsed >= config.heartbeatTicks()) {
                heartbeatElapsed = 0;
                broadcastAppend();
            }
        } else if (++electionElapsed >= electionTimeout) {
            startElection();
        }
    }

    /** Handles one incoming message. */
    public void step(Envelope envelope) {
        RaftMessage message = envelope.message();
        if (message.term() > storage.currentTerm()) {
            becomeFollower(message.term());
        }
        if (message instanceof RaftMessage.RequestVote m) {
            onRequestVote(envelope.from(), m);
        } else if (message instanceof RaftMessage.RequestVoteResponse m) {
            onRequestVoteResponse(envelope.from(), m);
        } else if (message instanceof RaftMessage.AppendEntries m) {
            onAppendEntries(envelope.from(), m);
        } else if (message instanceof RaftMessage.AppendEntriesResponse m) {
            onAppendEntriesResponse(envelope.from(), m);
        }
    }

    /**
     * Appends a command to the log if this node is the leader.
     *
     * @return the index the command will have if it commits, or -1 when this node is not the
     *         leader. A returned index is not a promise: the command is durable only once
     *         {@link #commitIndex()} reaches it while the entry at that index still has the
     *         term this leader gave it.
     */
    public long propose(byte[] command) {
        if (role != Role.LEADER) {
            return -1;
        }
        long index = storage.lastIndex() + 1;
        storage.append(LogEntry.command(index, storage.currentTerm(), command));
        advanceCommit();
        broadcastAppend();
        return index;
    }

    /**
     * Starts an election now instead of waiting for the election timer. Does nothing on a
     * leader.
     */
    public void campaign() {
        if (role != Role.LEADER) {
            startElection();
        }
    }

    /** Returns the messages waiting to be sent and clears the list. */
    public List<Envelope> drainOutbox() {
        List<Envelope> drained = new ArrayList<>(outbox);
        outbox.clear();
        return drained;
    }

    // ------------------------------------------------------------------ elections

    private void startElection() {
        storage.saveTermAndVote(storage.currentTerm() + 1, id);
        role = Role.CANDIDATE;
        leaderId = NO_LEADER;
        votes.clear();
        votes.add(id);
        resetElectionTimer();
        if (hasMajority(votes.size())) {
            becomeLeader();
            return;
        }
        long lastIndex = storage.lastIndex();
        for (int peer : peers) {
            send(peer, new RaftMessage.RequestVote(storage.currentTerm(), id, lastIndex,
                    storage.termAt(lastIndex)));
        }
    }

    private void onRequestVote(int from, RaftMessage.RequestVote m) {
        long term = storage.currentTerm();
        boolean granted = false;
        if (m.term() == term) {
            int votedFor = storage.votedFor();
            boolean canVote = votedFor == RaftStorage.NO_VOTE || votedFor == m.candidateId();
            if (canVote && candidateLogIsUpToDate(m.lastLogIndex(), m.lastLogTerm())) {
                storage.saveTermAndVote(term, m.candidateId());
                resetElectionTimer();
                granted = true;
            }
        }
        send(from, new RaftMessage.RequestVoteResponse(term, granted));
    }

    /** A vote goes only to a candidate whose log is at least as complete as this node's. */
    private boolean candidateLogIsUpToDate(long candidateLastIndex, long candidateLastTerm) {
        long lastIndex = storage.lastIndex();
        long lastTerm = storage.termAt(lastIndex);
        return candidateLastTerm > lastTerm
                || (candidateLastTerm == lastTerm && candidateLastIndex >= lastIndex);
    }

    private void onRequestVoteResponse(int from, RaftMessage.RequestVoteResponse m) {
        if (role != Role.CANDIDATE || m.term() != storage.currentTerm() || !m.granted()) {
            return;
        }
        votes.add(from);
        if (hasMajority(votes.size())) {
            becomeLeader();
        }
    }

    private void becomeFollower(long term) {
        if (term != storage.currentTerm()) {
            storage.saveTermAndVote(term, RaftStorage.NO_VOTE);
        }
        role = Role.FOLLOWER;
        leaderId = NO_LEADER;
        resetElectionTimer();
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = id;
        heartbeatElapsed = 0;
        long next = storage.lastIndex() + 1;
        for (int peer : peers) {
            nextIndex.put(peer, next);
            matchIndex.put(peer, 0L);
        }
        // A leader may only count replicas for entries of its own term, so it appends one
        // immediately. Committing it also commits everything before it.
        storage.append(LogEntry.noop(next, storage.currentTerm()));
        advanceCommit();
        broadcastAppend();
    }

    // ------------------------------------------------------------------ replication

    private void broadcastAppend() {
        for (int peer : peers) {
            sendAppend(peer);
        }
    }

    private void sendAppend(int peer) {
        long next = nextIndex.get(peer);
        long prev = next - 1;
        send(peer, new RaftMessage.AppendEntries(storage.currentTerm(), id, prev,
                storage.termAt(prev), storage.entriesFrom(next, config.maxEntriesPerAppend()),
                commitIndex));
    }

    private void onAppendEntries(int from, RaftMessage.AppendEntries m) {
        long term = storage.currentTerm();
        if (m.term() < term) {
            send(from, new RaftMessage.AppendEntriesResponse(term, false, 0, storage.lastIndex() + 1));
            return;
        }
        // Same term: the sender is the one legitimate leader of this term.
        role = Role.FOLLOWER;
        leaderId = m.leaderId();
        resetElectionTimer();

        long prev = m.prevLogIndex();
        if (prev > storage.lastIndex()) {
            send(from, new RaftMessage.AppendEntriesResponse(term, false, 0, storage.lastIndex() + 1));
            return;
        }
        if (storage.termAt(prev) != m.prevLogTerm()) {
            // Skip the whole conflicting term in one round trip instead of one entry at a time.
            long conflictTerm = storage.termAt(prev);
            long conflictIndex = prev;
            while (conflictIndex > commitIndex + 1 && storage.termAt(conflictIndex - 1) == conflictTerm) {
                conflictIndex--;
            }
            send(from, new RaftMessage.AppendEntriesResponse(term, false, 0, conflictIndex));
            return;
        }
        for (LogEntry entry : m.entries()) {
            if (entry.index() <= storage.lastIndex()) {
                if (storage.termAt(entry.index()) == entry.term()) {
                    continue; // already have it (a duplicate or reordered message)
                }
                if (entry.index() <= commitIndex) {
                    throw new IllegalStateException("node " + id + " asked to overwrite committed "
                            + storage.entry(entry.index()) + " with " + entry);
                }
                storage.truncateFrom(entry.index());
            }
            storage.append(entry);
        }
        // Only entries up to here are known to match the leader; anything after may be stale.
        long match = prev + m.entries().size();
        if (m.leaderCommit() > commitIndex) {
            commitIndex = Math.max(commitIndex, Math.min(m.leaderCommit(), match));
            applyCommitted();
        }
        send(from, new RaftMessage.AppendEntriesResponse(term, true, match, 0));
    }

    private void onAppendEntriesResponse(int from, RaftMessage.AppendEntriesResponse m) {
        if (role != Role.LEADER || m.term() != storage.currentTerm()) {
            return;
        }
        long match = matchIndex.get(from);
        if (m.success()) {
            if (m.matchIndex() > match) {
                matchIndex.put(from, m.matchIndex());
                nextIndex.put(from, Math.max(nextIndex.get(from), m.matchIndex() + 1));
                advanceCommit();
            }
            if (nextIndex.get(from) <= storage.lastIndex()) {
                sendAppend(from); // the follower is still behind: keep it catching up
            }
        } else {
            // Back off, but never below what the follower has already confirmed: a late
            // failure reply must not undo progress.
            long retry = Math.min(m.conflictIndex(), nextIndex.get(from) - 1);
            nextIndex.put(from, Math.max(match + 1, Math.max(1, retry)));
            sendAppend(from);
        }
    }

    /** Commits the highest index that a majority holds, if it is from the current term. */
    private void advanceCommit() {
        long[] matches = new long[peers.size() + 1];
        int i = 0;
        for (int peer : peers) {
            matches[i++] = matchIndex.get(peer);
        }
        matches[i] = storage.lastIndex();
        Arrays.sort(matches);
        // After sorting, the entry at this position is held by a majority of nodes.
        long majorityIndex = matches[(matches.length - 1) / 2];
        if (majorityIndex > commitIndex && storage.termAt(majorityIndex) == storage.currentTerm()) {
            commitIndex = majorityIndex;
            applyCommitted();
        }
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            LogEntry entry = storage.entry(++lastApplied);
            if (!entry.noop()) {
                stateMachine.apply(entry.index(), entry.command());
            }
        }
    }

    // ------------------------------------------------------------------ helpers

    private boolean hasMajority(int count) {
        return count > (peers.size() + 1) / 2;
    }

    private void resetElectionTimer() {
        electionElapsed = 0;
        electionTimeout = config.electionMinTicks()
                + random.nextInt(config.electionMaxTicks() - config.electionMinTicks() + 1);
    }

    private void send(int to, RaftMessage message) {
        outbox.add(new Envelope(id, to, message));
    }

    // ------------------------------------------------------------------ observation

    public int id() {
        return id;
    }

    public Role role() {
        return role;
    }

    public boolean isLeader() {
        return role == Role.LEADER;
    }

    /** The leader this node currently follows, or {@link #NO_LEADER}. */
    public int leaderId() {
        return leaderId;
    }

    public long currentTerm() {
        return storage.currentTerm();
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long lastApplied() {
        return lastApplied;
    }

    public long lastLogIndex() {
        return storage.lastIndex();
    }
}
