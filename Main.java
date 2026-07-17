public class Main {
    public static void main(String[] args) throws InterruptedException {
        MessageStore store = new InMemoryMessageStore();
        QueueManager queueManager = new QueueManager(store);
        Dispatcher dispatcher = new Dispatcher(queueManager);

        PulseQClient client = PulseQClient.connect(queueManager, dispatcher);

        VisibilityTimeoutChecker checker = new VisibilityTimeoutChecker(queueManager);
        checker.start();

        System.out.println("=== PulseQ Broker SDK Demo ===\n");

        client.subscribe("orders", message -> {
            System.out.println("[Orders] Received: " + new String(message.getPayload()) + " (attempt " + message.getDeliveryAttempts() + ")");
            client.ack(message.getId(), "orders");
        });

        client.publish("orders", "First order".getBytes());
        client.publish("orders", "Second order".getBytes());

        Thread.sleep(500);

        client.subscribe("retry-test", message -> {
            System.out.println("[RetryTest] Received: " + new String(message.getPayload()) + " (attempt " + message.getDeliveryAttempts() + ")");
            client.nack(message.getId(), "retry-test", Reason.FAILED);
        });

        client.publish("retry-test", "Will be retried".getBytes());

        Thread.sleep(1000);

        System.out.println("\nOrder queue size after ACKs: " + queueManager.getQueue("orders").size());
        System.out.println("Orders DLQ: " + queueManager.getQueue("orders").getDeadLetterQueue().size());

        int retryDLQ = queueManager.getQueue("retry-test").getDeadLetterQueue().size();
        System.out.println("Retry-test DLQ after timeouts: " + retryDLQ);

        checker.stop();
        client.close();

        System.out.println("\n=== Demo Complete ===");
        System.exit(0);
    }
}
