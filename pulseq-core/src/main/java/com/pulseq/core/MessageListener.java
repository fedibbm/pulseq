package com.pulseq.core;

@FunctionalInterface
public interface MessageListener {
    void onMessage(Message message);
}
