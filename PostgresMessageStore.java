import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PostgresMessageStore implements MessageStore {
    private final String url;
    private final String user;
    private final String password;

    public PostgresMessageStore(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    @Override
    public void save(Message message) {
        String sql = "INSERT INTO messages (id, topic, payload, published_at, status, delivery_attempts, visibility_expires_at, max_retries) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                + "ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, delivery_attempts = EXCLUDED.delivery_attempts, visibility_expires_at = EXCLUDED.visibility_expires_at";
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, message.getId());
            stmt.setString(2, message.getTopic());
            stmt.setBytes(3, message.getPayload());
            stmt.setLong(4, message.getPublishedAt());
            stmt.setString(5, message.getStatus().name());
            stmt.setInt(6, message.getDeliveryAttempts());
            stmt.setLong(7, message.getVisibilityExpiresAt());
            stmt.setInt(8, message.getMaxRetries());
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to save message", e);
        }
    }

    @Override
    public void markInFlight(String messageId, long visibilityExpiresAt) {
        String sql = "UPDATE messages SET status = ?, visibility_expires_at = ? WHERE id = ?";
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, MessageStatus.IN_FLIGHT.name());
            stmt.setLong(2, visibilityExpiresAt);
            stmt.setString(3, messageId);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark message in-flight", e);
        }
    }

    @Override
    public void markAcknowledged(String messageId) {
        String sql = "UPDATE messages SET status = ? WHERE id = ?";
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, MessageStatus.ACKNOWLEDGED.name());
            stmt.setString(2, messageId);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to acknowledge message", e);
        }
    }

    @Override
    public void markDeadLettered(String messageId) {
        String sql = "UPDATE messages SET status = ? WHERE id = ?";
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, MessageStatus.DEAD_LETTERED.name());
            stmt.setString(2, messageId);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to dead-letter message", e);
        }
    }

    @Override
    public List<Message> loadAllAvailable() {
        String sql = "SELECT * FROM messages WHERE status IN (?, ?)";
        List<Message> result = new ArrayList<>();
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, MessageStatus.AVAILABLE.name());
            stmt.setString(2, MessageStatus.IN_FLIGHT.name());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    result.add(mapRow(rs));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load messages", e);
        }
        return result;
    }

    private Message mapRow(ResultSet rs) throws SQLException {
        return new Message(
                rs.getString("id"),
                rs.getString("topic"),
                rs.getBytes("payload"),
                rs.getLong("published_at"),
                rs.getLong("visibility_expires_at"),
                rs.getInt("delivery_attempts"),
                rs.getInt("max_retries"),
                MessageStatus.valueOf(rs.getString("status"))
        );
    }
}
