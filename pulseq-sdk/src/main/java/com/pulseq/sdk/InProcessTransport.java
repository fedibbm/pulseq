package com.pulseq.sdk;

import com.pulseq.core.Dispatcher;
import com.pulseq.core.Message;
import com.pulseq.core.QueueManager;
import com.pulseq.core.Reason;

import java.util.UUID;

/**
 * Embedded transport: the client and the broker share the same JVM, so operations are
 * direct method calls. Useful for testing and for the SDK's embedded-server mode.
 */
public class InProcessTransport implements ClientTransport {
    private final QueueManager queueManager;
    private final Dispatcher dispatcher;

    public InProcessTransport(QueueManager queueManager, Dispatcher dispatcher) {
        this.queueManager = queueManager;
        this.dispatcher = dispatcher;
    }

    @Override
    public String publish(String topic, byte[] payload) {
        String id = UUID.randomUUID().toString();
        Message message = new Message(id, topic, payload);
        queueManager.publish(topic, message);
        return id;
    }

    @Override
    public void subscribe(String topic, MessageHandler handler) {
        subscribe(topic, null, handler);
    }

    @Override
    public void subscribe(String topic, String groupId, MessageHandler handler) {
        dispatcher.subscribe(topic, groupId, handler::onMessage);
    }

    @Override
    public void ack(String messageId, String topic) {
        queueManager.getQueue(topic).ack(messageId);
    }

    @Override
    public void nack(String messageId, String topic, Reason reason) {
        queueManager.getQueue(topic).nack(messageId, reason);
    }

    @Override
    public void close() {
    }
}
