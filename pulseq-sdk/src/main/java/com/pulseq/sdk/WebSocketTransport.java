package com.pulseq.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulseq.core.Message;
import com.pulseq.core.Reason;
import jakarta.websocket.CloseReason;
import jakarta.websocket.ContainerProvider;
import jakarta.websocket.Endpoint;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.Session;
import jakarta.websocket.WebSocketContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Network transport: publishes over REST and consumes over WebSocket.
 *
 * <p>Each subscription maintains its own WebSocket connection. If a connection drops it is
 * re-established automatically with exponential backoff (see {@link ReconnectPolicy}) and the
 * subscription resumes.</p>
 */
public class WebSocketTransport implements ClientTransport {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String httpBase;
    private final String wsBase;
    private final HttpClient http;
    private final ReconnectPolicy reconnectPolicy;
    private final CopyOnWriteArrayList<SubscriptionSession> sessions = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public WebSocketTransport(String baseUrl) {
        this(baseUrl, new ReconnectPolicy(1_000, 30_000));
    }

    public WebSocketTransport(String baseUrl, ReconnectPolicy reconnectPolicy) {
        this.httpBase = baseUrl;
        this.wsBase = baseUrl.replaceFirst("^http", "ws");
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        this.reconnectPolicy = reconnectPolicy;
    }

    @Override
    public String publish(String topic, byte[] payload) {
        try {
            String body = JSON.writeValueAsString(Map.of("payload", new String(payload, StandardCharsets.UTF_8)));
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(httpBase + "/publish/" + topic))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() / 100 != 2) {
                throw new RuntimeException("Publish failed: HTTP " + response.statusCode() + " " + response.body());
            }
            return JSON.readTree(response.body()).path("messageId").asText(null);
        } catch (Exception e) {
            throw new RuntimeException("Publish failed for topic '" + topic + "'", e);
        }
    }

    @Override
    public void subscribe(String topic, MessageHandler handler) {
        subscribe(topic, null, handler);
    }

    @Override
    public void subscribe(String topic, String groupId, MessageHandler handler) {
        SubscriptionSession session = new SubscriptionSession(topic, groupId, handler);
        sessions.add(session);
        session.start();
    }

    @Override
    public void ack(String messageId, String topic) {
        send(topic, "ACK " + messageId);
    }

    @Override
    public void nack(String messageId, String topic, Reason reason) {
        send(topic, "NACK " + messageId + " " + reason.name());
    }

    private void send(String topic, String payload) {
        for (SubscriptionSession session : sessions) {
            if (session.topic.equals(topic)) {
                session.send(payload);
                return;
            }
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (SubscriptionSession session : sessions) {
                session.close();
            }
        }
    }

    private final class SubscriptionSession extends Endpoint {
        private final String topic;
        private final String groupId;
        private final MessageHandler handler;
        private final ScheduledExecutorService executor;
        private volatile Session session;
        private volatile boolean running = true;

        SubscriptionSession(String topic, String groupId, MessageHandler handler) {
            this.topic = topic;
            this.groupId = groupId;
            this.handler = handler;
            this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "pulseq-reconnect-" + topic);
                t.setDaemon(true);
                return t;
            });
        }

        void start() {
            executor.schedule(this::connect, 0, TimeUnit.MILLISECONDS);
        }

        void connect() {
            if (!running) return;
            try {
                WebSocketContainer container = ContainerProvider.getWebSocketContainer();
                String uri = wsBase + "/subscribe/" + topic + (groupId != null ? "?group=" + groupId : "");
                session = container.connectToServer(this, URI.create(uri));
            } catch (Exception e) {
                System.err.println("PulseQ: reconnect to '" + topic + "' failed: " + e.getMessage());
                scheduleReconnect();
            }
        }

        void scheduleReconnect() {
            if (!running) return;
            long delay = reconnectPolicy.nextDelayMillis();
            executor.schedule(this::connect, delay, TimeUnit.MILLISECONDS);
        }

        void send(String payload) {
            Session s = session;
            if (s != null && s.isOpen()) {
                try {
                    s.getBasicRemote().sendText(payload);
                } catch (Exception e) {
                    System.err.println("PulseQ: failed to send '" + payload + "': " + e);
                }
            }
        }

        @Override
        public void onOpen(Session session, EndpointConfig config) {
            reconnectPolicy.reset();
            session.addMessageHandler(String.class, this::onMessage);
        }

        public void onMessage(String text) {
            try {
                JsonNode node = JSON.readTree(text);
                String id = node.path("id").asText();
                byte[] payload = Base64.getDecoder().decode(node.path("payload").asText(""));
                int attempts = node.path("deliveryAttempts").asInt();
                handler.onMessage(Message.delivery(id, topic, payload, attempts));
            } catch (Exception e) {
                System.err.println("PulseQ: failed to parse delivery message: " + text + " (" + e + ")");
            }
        }

        @Override
        public void onClose(Session session, CloseReason reason) {
            scheduleReconnect();
        }

        @Override
        public void onError(Session session, Throwable throwable) {
            try {
                if (session != null) session.close();
            } catch (Exception ignore) {
                // already closed
            }
        }

        void close() {
            running = false;
            try {
                Session s = session;
                if (s != null) s.close();
            } catch (Exception ignore) {
                // already closed
            }
            executor.shutdownNow();
        }
    }
}
