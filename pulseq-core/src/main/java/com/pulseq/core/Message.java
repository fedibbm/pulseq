package com.pulseq.core;

/**
 * A single message flowing through a PulseQ topic queue.
 *
 * <p>A message is created with a unique id, a topic and a byte payload. It moves through
 * the lifecycle {@link MessageStatus#AVAILABLE} -> {@link MessageStatus#IN_FLIGHT} and is
 * terminated either by an explicit ack/nack from a consumer or by exhausting its retries,
 * at which point it is routed to the topic's dead-letter queue.</p>
 *
 * <p>Delivery guarantees are at-least-once: a message may be delivered more than once if
 * the consumer's visibility timeout elapses before an ack is received.</p>
 */
public class Message {
    private final String id;
    private final String topic;
    private final byte[] payload;
    private final long publishedAt;
    private final int maxRetries;
    private final long ttlMillis;
    private long visibilityExpiresAt;
    private int deliveryAttempts;
    private MessageStatus status;
    private long retryAt;

    /**
     * Creates a message with the default retry policy (3 attempts) and no TTL.
     */
    public Message(String id, String topic, byte[] payload) {
        this(id, topic, payload, 3, 0);
    }

    /**
     * Creates a message with a custom retry policy and no TTL.
     */
    public Message(String id, String topic, byte[] payload, int maxRetries) {
        this(id, topic, payload, maxRetries, 0);
    }

    /**
     * Creates a message with a custom TTL. Expired available messages are dropped by the broker.
     *
     * @param ttlMillis message time-to-live in milliseconds, or 0 for no expiration
     */
    public Message(String id, String topic, byte[] payload, long ttlMillis) {
        this(id, topic, payload, 3, ttlMillis);
    }

    /**
     * Creates a message with a custom retry policy and TTL.
     *
     * @param ttlMillis message time-to-live in milliseconds, or 0 for no expiration
     */
    public Message(String id, String topic, byte[] payload, int maxRetries, long ttlMillis) {
        this.id = id;
        this.topic = topic;
        this.payload = payload;
        this.publishedAt = System.currentTimeMillis();
        this.maxRetries = maxRetries;
        this.ttlMillis = ttlMillis;
        this.status = MessageStatus.AVAILABLE;
        this.deliveryAttempts = 0;
        this.visibilityExpiresAt = 0;
        this.retryAt = 0;
    }

    Message(String id, String topic, byte[] payload, long publishedAt, long visibilityExpiresAt,
            int deliveryAttempts, int maxRetries, MessageStatus status, long ttlMillis) {
        this.id = id;
        this.topic = topic;
        this.payload = payload;
        this.publishedAt = publishedAt;
        this.visibilityExpiresAt = visibilityExpiresAt;
        this.deliveryAttempts = deliveryAttempts;
        this.maxRetries = maxRetries;
        this.status = status;
        this.ttlMillis = ttlMillis;
        this.retryAt = 0;
    }

    public String getId() { return id; }
    public String getTopic() { return topic; }
    public byte[] getPayload() { return payload; }
    public String getPayloadAsString() { return new String(payload); }
    public long getPublishedAt() { return publishedAt; }
    public long getVisibilityExpiresAt() { return visibilityExpiresAt; }
    public int getDeliveryAttempts() { return deliveryAttempts; }
    public int getMaxRetries() { return maxRetries; }
    public MessageStatus getStatus() { return status; }
    public long getTtlMillis() { return ttlMillis; }

    /**
     * Creates a lightweight delivery snapshot for a consumer, carrying the delivery attempt
     * count received from the broker. Used by client transports when rehydrating messages
     * from the wire.
     */
    public static Message delivery(String id, String topic, byte[] payload, int deliveryAttempts) {
        Message message = new Message(id, topic, payload);
        message.deliveryAttempts = deliveryAttempts;
        return message;
    }

    /**
     * Returns true when a TTL was configured and the message has outlived it.
     */
    public boolean isExpired() {
        return ttlMillis > 0 && (System.currentTimeMillis() - publishedAt) > ttlMillis;
    }

    void setStatus(MessageStatus status) { this.status = status; }
    void setVisibilityExpiresAt(long visibilityExpiresAt) { this.visibilityExpiresAt = visibilityExpiresAt; }
    void incrementDeliveryAttempts() { this.deliveryAttempts++; }
    void resetDeliveryAttempts() { this.deliveryAttempts = 0; }

    long getRetryAt() { return retryAt; }
    void setRetryAt(long retryAt) { this.retryAt = retryAt; }

    @Override
    public String toString() {
        return "Message{id='" + id + "', topic='" + topic + "', status=" + status
                + ", deliveryAttempts=" + deliveryAttempts + ", maxRetries=" + maxRetries + "}";
    }
}
