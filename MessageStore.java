import java.util.List;

public interface MessageStore {
    void save(Message message);
    void markInFlight(String messageId, long visibilityExpiresAt);
    void markAcknowledged(String messageId);
    void markDeadLettered(String messageId);
    List<Message> loadAllAvailable();
}
