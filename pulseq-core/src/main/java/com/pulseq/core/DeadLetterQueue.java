package com.pulseq.core;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class DeadLetterQueue {
    private final String sourceTopic;
    private final LinkedList<Message> messages;

    DeadLetterQueue(String sourceTopic) {
        this.sourceTopic = sourceTopic;
        this.messages = new LinkedList<>();
    }

    void add(Message message) {
        this.messages.addLast(message);
    }

    List<Message> list() {
        return new ArrayList<>(this.messages);
    }

    public String getSourceTopic() { return this.sourceTopic; }
    public int size() { return this.messages.size(); }
}
