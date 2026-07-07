import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class MessageQueue {
    private String topic;
    private LinkedList<Message> messages;
    private int capacity;
    private ReentrantLock lock;
    private Condition notEmpty;
    private Condition notFull;
    private final int visibilityTimeout = 30_000;
    private Map<String, Message> inFlight;
    private DeadLetterQueue deadLetterQueue;
    private PriorityQueue<Message> timeoutQueue;

    public MessageQueue(String topic, int capacity) {
        this.topic = topic;
        this.capacity = capacity;
        this.lock = new ReentrantLock();
        this.notEmpty = lock.newCondition();
        this.notFull = lock.newCondition();
        this.messages = new LinkedList<>();
        this.inFlight = new HashMap<>();
        this.deadLetterQueue = new DeadLetterQueue(topic);
        this.timeoutQueue = new PriorityQueue<>(Comparator.comparingLong(Message::getVisibilityExpiresAt));
    }


    void enqueue(Message message) {
        lock.lock();
        try {
            while (messages.size() >= capacity) {
                notFull.await();
            }

            messages.addLast(message);
            notEmpty.signal();
        } catch (java.lang.InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }

    }

    Message dequeue() {
        lock.lock();
        Message message = null;
        try {
            while (messages.isEmpty()) {
                notEmpty.await();
            }
            message = messages.removeFirst();
            message.setStatus(MessageStatus.IN_FLIGHT);
            this.inFlight.put(message.getId(), message);
            message.setVisibilityExpiresAt(System.currentTimeMillis() + visibilityTimeout);
            this.timeoutQueue.add(message);
            notFull.signal();
        } catch (java.lang.InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
        return message;
    }

    boolean ack(String messageId) {
        lock.lock();
        Message message = inFlight.get(messageId);
        try {
            if (message != null) {
                inFlight.remove(messageId);
                message.setStatus(MessageStatus.ACKNOWLEDGED);
            }
        } finally {
            lock.unlock();
        }
        return message != null;
    }

    boolean nack(String messageId, Reason reason) {
        lock.lock();
        Message message = inFlight.get(messageId);
        try {
            if (message != null) {
                message.incrementDeliveryAttempts();
                if (reason == Reason.REJECTED) {
                    message.setStatus(MessageStatus.DEAD_LETTERED);
                    this.deadLetterQueue.add(message);
                    inFlight.remove(messageId);
                }
                else if (reason == Reason.FAILED) {
                    if (message.getDeliveryAttempts() >= message.getMaxRetries()) {
                        message.setStatus(MessageStatus.DEAD_LETTERED);
                        this.deadLetterQueue.add(message);
                    } else {
                        message.setStatus(MessageStatus.AVAILABLE);
                        this.messages.addFirst(message);
                        notEmpty.signal();
                    }
                    this.inFlight.remove(messageId);
                }
            }
        } finally {
            lock.unlock();
        }
        return message != null;
    }

    void requeueTimedOut() {
        lock.lock();
        try{
            while (!timeoutQueue.isEmpty()) {
                Message head = timeoutQueue.peek();
                if (this.inFlight.get(head.getId()) == null) {
                    timeoutQueue.poll();
                    continue;
                }
                if (head.getVisibilityExpiresAt() > System.currentTimeMillis()) break;
                head = this.timeoutQueue.poll();
                head.incrementDeliveryAttempts();
                if (head.getDeliveryAttempts() >= head.getMaxRetries()) {
                    head.setStatus(MessageStatus.DEAD_LETTERED);
                    this.deadLetterQueue.add(head);
                } else {
                    head.setStatus(MessageStatus.AVAILABLE);
                    this.messages.add(head);
                    notEmpty.signal();
                }
            }
        }finally {
            lock.unlock();
        }
    }


}
