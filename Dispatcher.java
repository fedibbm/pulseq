import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Dispatcher {

    private Map<String, List<MessageListener>> sessions;
    private QueueManager queueManager;
    private Map<String, Thread> threads;

    public Dispatcher(QueueManager queueManager) {
        this.queueManager = queueManager;
        this.sessions = new ConcurrentHashMap<>();
        this.threads = new ConcurrentHashMap<>();
    }

    void subscribe(String topic, MessageListener session) {
        queueManager.createQueue(topic);
        if (!this.sessions.containsKey(topic)) {
            this.sessions.put(topic, new CopyOnWriteArrayList<>());
        }
        boolean firstSubscriber = this.sessions.get(topic).isEmpty();
        this.sessions.get(topic).add(session);
        if (firstSubscriber) {
            Thread consumer = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    MessageQueue queue = queueManager.getQueue(topic);
                    Message message = queue.dequeue();
                    if (message == null) break;
                    dispatch(topic, message);
                }
            });
            this.threads.put(topic, consumer);
            consumer.start();
        }
    }

    void unsubscribe(String topic, MessageListener session) {
        if (!this.threads.containsKey(topic)) return;
        Thread consumerThread = this.threads.get(topic);
        List<MessageListener> listeners = this.sessions.get(topic);
        if (listeners == null) return;
        listeners.remove(session);
        if (listeners.isEmpty()) {
            consumerThread.interrupt();
            threads.remove(topic);
        }

    }

    void dispatch(String topic, Message message) {
        List<MessageListener> listeners = this.sessions.get(topic);
        if (listeners == null) return;
        for (MessageListener listener : listeners) {
            listener.onMessage(message);
        }

    }

    void onSessionClosed(MessageListener session) {
        for(String topic : this.sessions.keySet()){
            unsubscribe(topic, session);
        }
    }
}
