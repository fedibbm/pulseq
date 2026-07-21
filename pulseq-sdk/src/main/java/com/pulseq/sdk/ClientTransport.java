package com.pulseq.sdk;

import com.pulseq.core.Reason;

public interface ClientTransport {
    void publish(String topic, byte[] payload);
    void subscribe(String topic, MessageHandler handler);
    void ack(String messageId, String topic);
    void nack(String messageId, String topic, Reason reason);
    void close();
}
