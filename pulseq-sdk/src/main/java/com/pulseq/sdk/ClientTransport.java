package com.pulseq.sdk;

import com.pulseq.core.Reason;

/**
 * Abstraction over how a client talks to the broker. Implementations include the embedded
 * in-process transport and a WebSocket-based network transport with auto-reconnect.
 */
public interface ClientTransport {

    /**
     * Publishes a message to a topic.
     *
     * @param topic   the topic to publish to (auto-created on the broker)
     * @param payload the raw byte payload
     * @return the broker-assigned message id
     */
    String publish(String topic, byte[] payload);

    /**
     * Subscribes to a topic. Every subscriber receives a copy of each message (fan-out).
     *
     * @param topic   the topic to consume
     * @param handler callback invoked for each delivered message
     */
    void subscribe(String topic, MessageHandler handler);

    /**
     * Subscribes to a topic as part of a consumer group. Messages are shared round-robin
     * between all subscribers in the same group (competing consumers).
     *
     * @param topic   the topic to consume
     * @param groupId the consumer group id
     * @param handler callback invoked for each delivered message
     */
    void subscribe(String topic, String groupId, MessageHandler handler);

    /**
     * Acknowledges a message, completing its delivery.
     *
     * @param messageId the id from the delivered message
     * @param topic     the message's topic
     */
    void ack(String messageId, String topic);

    /**
     * Negatively acknowledges a message. {@code REJECTED} dead-letters it immediately,
     * {@code FAILED} schedules a retry with backoff.
     *
     * @param messageId the id from the delivered message
     * @param topic     the message's topic
     * @param reason    {@link Reason#FAILED} or {@link Reason#REJECTED}
     */
    void nack(String messageId, String topic, Reason reason);

    /**
     * Releases any resources held by the transport (sockets, threads).
     */
    void close();
}
