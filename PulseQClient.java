public class PulseQClient {
    private final ClientTransport transport;

    private PulseQClient(ClientTransport transport) {
        this.transport = transport;
    }

    public static PulseQClient connect(QueueManager queueManager, Dispatcher dispatcher) {
        return new PulseQClient(new InProcessTransport(queueManager, dispatcher));
    }

    public void publish(String topic, byte[] payload) {
        transport.publish(topic, payload);
    }

    public void subscribe(String topic, MessageHandler handler) {
        transport.subscribe(topic, handler);
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
