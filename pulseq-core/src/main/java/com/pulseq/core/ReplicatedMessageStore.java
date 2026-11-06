package com.pulseq.core;

import com.pulseq.raft.NotLeaderException;
import com.pulseq.raft.RaftRuntime;
import com.pulseq.raft.StateMachine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@link MessageStore} shared by a cluster of brokers. Every change goes through Raft and
 * returns only once a majority of brokers hold it; each broker then applies the change to its
 * own local store. Any broker can therefore take over with a complete store.
 *
 * <p>Only the leader can write. On any other broker a write fails with
 * {@link NotLeaderException}, which names the leader when it is known.</p>
 *
 * <p>In-flight marks are not replicated. They stay on the leader, so after a failover a
 * message that was being processed is delivered again, which at-least-once delivery already
 * allows. This saves a round trip to the other brokers on every delivery.</p>
 */
public final class ReplicatedMessageStore implements MessageStore, StateMachine {

    private static final byte SAVE = 1;
    private static final byte ACKNOWLEDGE = 2;
    private static final byte DEAD_LETTER = 3;
    private static final byte SWEEP = 4;

    private final MessageStore local;
    private final long commitTimeoutMillis;
    private final Map<Long, Integer> sweepResults = new ConcurrentHashMap<>();
    private volatile RaftRuntime raft;

    /**
     * @param local               this broker's own store, written only through committed changes
     * @param commitTimeoutMillis how long a write waits for a majority before giving up
     */
    public ReplicatedMessageStore(MessageStore local, long commitTimeoutMillis) {
        this.local = local;
        this.commitTimeoutMillis = commitTimeoutMillis;
    }

    /** Connects the store to its Raft node. The node must use this store as its state machine. */
    public void attach(RaftRuntime raft) {
        this.raft = raft;
    }

    // ------------------------------------------------------------------ writes (leader only)

    @Override
    public void save(Message message) {
        replicate(encodeSave(message));
    }

    @Override
    public void markAcknowledged(String messageId) {
        replicate(encodeId(ACKNOWLEDGE, messageId));
    }

    @Override
    public void markDeadLettered(String messageId) {
        replicate(encodeId(DEAD_LETTER, messageId));
    }

    @Override
    public void markInFlight(String messageId, long visibilityExpiresAt) {
        local.markInFlight(messageId, visibilityExpiresAt);
    }

    @Override
    public int sweepCompleted(long cutoffMillis) {
        long index = replicate(encodeSweep(cutoffMillis));
        Integer removed = sweepResults.remove(index);
        return removed == null ? 0 : removed;
    }

    private long replicate(byte[] command) {
        RaftRuntime node = raft;
        if (node == null) {
            throw new IllegalStateException("store is not attached to a Raft node");
        }
        try {
            return node.propose(command).get(commitTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException failure) {
                throw failure;
            }
            throw new ReplicationException("replication failed", e.getCause());
        } catch (TimeoutException e) {
            throw new ReplicationException("no majority confirmed the change within "
                    + commitTimeoutMillis + " ms", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ReplicationException("interrupted while waiting for a majority", e);
        }
    }

    // ------------------------------------------------------------------ reads (local)

    @Override
    public List<Message> loadAllAvailable() {
        return local.loadAllAvailable();
    }

    @Override
    public List<Message> loadDeadLettered() {
        return local.loadDeadLettered();
    }

    @Override
    public void close() {
        local.close();
    }

    // ------------------------------------------------------------------ applying committed changes

    /** Called by Raft on every broker, in the same order, once a change is committed. */
    @Override
    public void apply(long index, byte[] command) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(command))) {
            byte operation = in.readByte();
            switch (operation) {
                case SAVE -> local.save(readMessage(in));
                case ACKNOWLEDGE -> local.markAcknowledged(in.readUTF());
                case DEAD_LETTER -> local.markDeadLettered(in.readUTF());
                case SWEEP -> {
                    int removed = local.sweepCompleted(in.readLong());
                    RaftRuntime node = raft;
                    if (node != null && node.isLeader()) {
                        sweepResults.put(index, removed); // only a waiting caller reads it
                    }
                }
                default -> throw new IllegalStateException("unknown store operation " + operation);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ------------------------------------------------------------------ encoding

    private static byte[] encodeSave(Message message) {
        return encode(SAVE, out -> {
            out.writeUTF(message.getId());
            out.writeUTF(message.getTopic());
            out.writeInt(message.getPayload().length);
            out.write(message.getPayload());
            out.writeLong(message.getPublishedAt());
            out.writeInt(message.getDeliveryAttempts());
            out.writeInt(message.getMaxRetries());
            out.writeByte(message.getStatus().ordinal());
            out.writeLong(message.getTtlMillis());
        });
    }

    private static Message readMessage(DataInputStream in) throws IOException {
        String id = in.readUTF();
        String topic = in.readUTF();
        byte[] payload = in.readNBytes(in.readInt());
        long publishedAt = in.readLong();
        int deliveryAttempts = in.readInt();
        int maxRetries = in.readInt();
        MessageStatus status = MessageStatus.values()[in.readByte()];
        long ttlMillis = in.readLong();
        return new Message(id, topic, payload, publishedAt, 0, deliveryAttempts, maxRetries,
                status, ttlMillis);
    }

    private static byte[] encodeId(byte operation, String messageId) {
        return encode(operation, out -> out.writeUTF(messageId));
    }

    private static byte[] encodeSweep(long cutoffMillis) {
        return encode(SWEEP, out -> out.writeLong(cutoffMillis));
    }

    private interface Body {
        void write(DataOutputStream out) throws IOException;
    }

    private static byte[] encode(byte operation, Body body) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeByte(operation);
            body.write(out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }
}
