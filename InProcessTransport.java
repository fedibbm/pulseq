import java.util.UUID;

public class InProcessTransport implements ClientTransport {
    private final QueueManager queueManager;
    private final Dispatcher dispatcher;

    public InProcessTransport(QueueManager queueManager, Dispatcher dispatcher) {
        this.queueManager = queueManager;
        this.dispatcher = dispatcher;
    }

    @Override
    public void publish(String topic, byte[] payload) {
        Message message = new Message(UUID.randomUUID().toString(), topic, payload);
        queueManager.publish(topic, message);
    }

    @Override
    public void subscribe(String topic, MessageHandler handler) {
        dispatcher.subscribe(topic, handler::onMessage);
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
