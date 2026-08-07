package com.pulseq.server;

import com.pulseq.core.Dispatcher;
import com.pulseq.core.Message;
import com.pulseq.core.MessageListener;
import com.pulseq.core.QueueManager;
import com.pulseq.core.Reason;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * WebSocket endpoint for consuming messages.
 *
 * <p>Connect to {@code /subscribe/{topic}[?group=myGroup]}. The server pushes each delivery
 * as JSON: {@code {"id":..., "topic":..., "deliveryAttempts":..., "payload":"<base64>"}}.
 * Consumers reply with {@code ACK <messageId>} or {@code NACK <messageId> <REASON>}.</p>
 */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final QueueManager queueManager;
    private final Dispatcher dispatcher;

    public WebSocketConfig(QueueManager queueManager, Dispatcher dispatcher) {
        this.queueManager = queueManager;
        this.dispatcher = dispatcher;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(new PulseQWebSocketHandler(queueManager, dispatcher), "/subscribe/{topic}")
                .setAllowedOrigins("*");
    }

    private static class PulseQWebSocketHandler extends TextWebSocketHandler {

        private final QueueManager queueManager;
        private final Dispatcher dispatcher;
        private final ConcurrentMap<String, Subscription> subscriptions = new ConcurrentHashMap<>();

        PulseQWebSocketHandler(QueueManager queueManager, Dispatcher dispatcher) {
            this.queueManager = queueManager;
            this.dispatcher = dispatcher;
        }

        @Override
        public void afterConnectionEstablished(WebSocketSession session) {
            URI uri = session.getUri();
            String path = uri.getPath();
            String topic = path.substring(path.lastIndexOf('/') + 1).trim();
            String group = queryParam(uri.getQuery(), "group");

            MessageListener listener = message -> sendDelivery(session, message);
            subscriptions.put(session.getId(), new Subscription(topic, listener));
            dispatcher.subscribe(topic, group, listener);
        }

        @Override
        protected void handleTextMessage(WebSocketSession session, TextMessage message) {
            Subscription sub = subscriptions.get(session.getId());
            if (sub == null) return;

            String payload = message.getPayload().trim();
            if (payload.startsWith("ACK")) {
                queueManager.getQueue(sub.topic).ack(payload.substring(3).trim());
            } else if (payload.startsWith("NACK")) {
                String[] parts = payload.substring(4).trim().split("\\s+");
                if (parts.length >= 1 && !parts[0].isEmpty()) {
                    String messageId = parts[0];
                    Reason reason = parts.length >= 2 ? Reason.valueOf(parts[1].toUpperCase()) : Reason.FAILED;
                    queueManager.getQueue(sub.topic).nack(messageId, reason);
                }
            }
        }

        @Override
        public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
            Subscription sub = subscriptions.remove(session.getId());
            if (sub != null) {
                dispatcher.onSessionClosed(sub.listener);
            }
        }

        private static void sendDelivery(WebSocketSession session, Message message) {
            if (!session.isOpen()) return;
            String json = "{\"id\":\"" + message.getId()
                    + "\",\"topic\":\"" + message.getTopic()
                    + "\",\"deliveryAttempts\":" + message.getDeliveryAttempts()
                    + ",\"payload\":\"" + Base64.getEncoder().encodeToString(message.getPayload()) + "\"}";
            synchronized (session) {
                if (!session.isOpen()) return;
                try {
                    session.sendMessage(new TextMessage(json));
                } catch (IOException e) {
                    // send failed (session closing); message is redelivered after its visibility timeout
                }
            }
        }

        private static String queryParam(String query, String name) {
            if (query == null) return null;
            for (String part : query.split("&")) {
                String[] kv = part.split("=", 2);
                if (kv.length == 2 && kv[0].equals(name)) {
                    return kv[1];
                }
            }
            return null;
        }

        private record Subscription(String topic, MessageListener listener) {
        }
    }
}
