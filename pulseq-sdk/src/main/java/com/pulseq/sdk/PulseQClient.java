package com.pulseq.sdk;

import com.pulseq.core.Dispatcher;
import com.pulseq.core.QueueManager;
import com.pulseq.core.Reason;

import java.nio.charset.StandardCharsets;

/**
 * High-level client for PulseQ.
 *
 * <p>Connects either in-process (embedded broker) or over the network to a running
 * {@code pulseq-server} via {@link #connect(String)}. Delivered messages are passed to the
 * registered {@link MessageHandler}; handlers are responsible for calling
 * {@link #ack(String, String)} or {@link #nack(String, String, Reason)}.</p>
 */
public class PulseQClient {

    private final ClientTransport transport;

    private PulseQClient(ClientTransport transport) {
        this.transport = transport;
    }

    /**
     * Creates an in-process client sharing the given broker (no network).
     */
    public static PulseQClient connect(QueueManager queueManager, Dispatcher dispatcher) {
        return new PulseQClient(new InProcessTransport(queueManager, dispatcher));
    }

    /**
     * Creates a network client that connects to a PulseQ server over HTTP/WebSocket with
     * automatic reconnect.
     *
     * @param baseUrl e.g. {@code http://localhost:8080}
     */
    public static PulseQClient connect(String baseUrl) {
        return new PulseQClient(new WebSocketTransport(baseUrl));
    }

    /**
     * Publishes a byte payload.
     *
     * @return the message id assigned by the broker
     */
    public String publish(String topic, byte[] payload) {
        return transport.publish(topic, payload);
    }

    /**
     * Publishes a UTF-8 string payload.
     *
     * @return the message id assigned by the broker
     */
    public String publish(String topic, String payload) {
        return transport.publish(topic, payload.getBytes(StandardCharsets.UTF_8));
    }

    public void subscribe(String topic, MessageHandler handler) {
        transport.subscribe(topic, handler);
    }

    /**
     * Subscribes to a topic as part of a consumer group. Messages on the topic are shared
     * (round-robin) between all subscribers in the same group.
     */
    public void subscribe(String topic, String groupId, MessageHandler handler) {
        transport.subscribe(topic, groupId, handler);
    }

    public void ack(String messageId, String topic) {
        transport.ack(messageId, topic);
    }

    public void nack(String messageId, String topic, Reason reason) {
        transport.nack(messageId, topic, reason);
    }

    public void close() {
        transport.close();
    }
}
