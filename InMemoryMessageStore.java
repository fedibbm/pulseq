import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryMessageStore implements MessageStore {
    private final Map<String, Message> store = new ConcurrentHashMap<>();

    @Override
    public void save(Message message) {
        store.put(message.getId(), message);
    }

    @Override
    public void markInFlight(String messageId, long visibilityExpiresAt) {
        Message message = store.get(messageId);
        if (message != null) {
            message.setStatus(MessageStatus.IN_FLIGHT);
            message.setVisibilityExpiresAt(visibilityExpiresAt);
        }
    }

    @Override
    public void markAcknowledged(String messageId) {
        Message message = store.get(messageId);
        if (message != null) {
            message.setStatus(MessageStatus.ACKNOWLEDGED);
        }
    }

    @Override
    public void markDeadLettered(String messageId) {
        Message message = store.get(messageId);
        if (message != null) {
            message.setStatus(MessageStatus.DEAD_LETTERED);
        }
    }

    @Override
    public List<Message> loadAllAvailable() {
        List<Message> result = new ArrayList<>();
        for (Message message : store.values()) {
            if (message.getStatus() == MessageStatus.AVAILABLE
                    || message.getStatus() == MessageStatus.IN_FLIGHT) {
                result.add(message);
            }
        }
        return result;
    }
}
