package com.pulseq.sdk;

import com.pulseq.core.Message;

@FunctionalInterface
public interface MessageHandler {
    void onMessage(Message message);
}
