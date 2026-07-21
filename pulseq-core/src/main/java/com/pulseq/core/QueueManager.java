package com.pulseq.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QueueManager {
    public static final int MAX_CAPACITY = 1000;
    private Map<String, MessageQueue> queues;
    private MessageStore store;

    public QueueManager(MessageStore store) {
        this.queues = new ConcurrentHashMap<>();
        this.store = store;
    }

    public void publish(String topic, Message message) {
        if (!this.queues.containsKey(topic)) {
            this.createQueue(topic);
        }
        getQueue(topic).enqueue(message);
    }

    public MessageQueue getQueue(String topic) {
        return this.queues.get(topic);
    }

    boolean createQueue(String topic) {
        if (!this.queues.containsKey(topic)) {
            this.queues.put(topic, new MessageQueue(topic, MAX_CAPACITY, store));
            return true;
        }
        return false;
    }

    void recover() {
        for (Message message : store.loadAllAvailable()) {
            message.setStatus(MessageStatus.AVAILABLE);
            publish(message.getTopic(), message);
        }
    }

    List<String> listTopics() {
        return new ArrayList<>(this.queues.keySet());
    }
}
