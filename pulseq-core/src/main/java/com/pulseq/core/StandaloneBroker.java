package com.pulseq.core;

/** A broker that runs alone: one set of queues for the life of the process. */
public final class StandaloneBroker implements Broker {

    private final QueueManager queues;
    private final Dispatcher dispatcher;

    public StandaloneBroker(QueueManager queues, Dispatcher dispatcher) {
        this.queues = queues;
        this.dispatcher = dispatcher;
    }

    @Override
    public QueueManager queues() {
        return queues;
    }

    @Override
    public void subscribe(String topic, String groupId, MessageListener listener) {
        dispatcher.subscribe(topic, groupId, listener);
    }

    @Override
    public void unsubscribe(MessageListener listener) {
        dispatcher.onSessionClosed(listener);
    }

    @Override
    public boolean isServing() {
        return true;
    }
}
