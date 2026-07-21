package com.pulseq.core;

public class Message {
    private String id;
    private String topic;
    private byte[] payload;
    private long publishedAt;
    private long visibilityExpiresAt;
    private int deliveryAttempts;
    private int maxRetries;
    private MessageStatus status;

    public Message(String id, String topic, byte[] payload) {
        this.id = id;
        this.topic = topic;
        this.payload = payload;
        this.publishedAt = System.currentTimeMillis();
        this.status = MessageStatus.AVAILABLE;
        this.deliveryAttempts = 0;
        this.maxRetries = 3;
        this.visibilityExpiresAt = 0;
    }

    Message(String id, String topic, byte[] payload, long publishedAt, long visibilityExpiresAt, int deliveryAttempts, int maxRetries, MessageStatus status) {
        this.id = id;
        this.topic = topic;
        this.payload = payload;
        this.publishedAt = publishedAt;
        this.visibilityExpiresAt = visibilityExpiresAt;
        this.deliveryAttempts = deliveryAttempts;
        this.maxRetries = maxRetries;
        this.status = status;
    }

    public String getId() { return id; }
    public String getTopic() { return topic; }
    public byte[] getPayload() { return payload; }
    public long getPublishedAt() { return publishedAt; }
    public long getVisibilityExpiresAt() { return visibilityExpiresAt; }
    public int getDeliveryAttempts() { return deliveryAttempts; }
    public int getMaxRetries() { return maxRetries; }
    public MessageStatus getStatus() { return status; }
    void setStatus(MessageStatus status) { this.status = status; }
    void setVisibilityExpiresAt(long visibilityExpiresAt) { this.visibilityExpiresAt = visibilityExpiresAt; }
    void incrementDeliveryAttempts() { this.deliveryAttempts++; }
}
