package com.pulseq.server;

import com.pulseq.core.QueueManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.web.client.RestClient;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end test of the WebSocket consume path against a real embedded server.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WebSocketE2ETest {

    @LocalServerPort
    private int port;

    @Autowired
    private QueueManager queueManager;

    private final List<WebSocketSession> openSessions = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() {
        openSessions.forEach(session -> {
            try {
                session.close();
            } catch (Exception ignore) {
                // already closed
            }
        });
    }

    @Test
    void publishesOverRestAndConsumesOverWebSocketWithAck() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        List<String> messages = new CopyOnWriteArrayList<>();

        WebSocketSession session = connect("orders", null, received, messages);
        openSessions.add(session);

        RestClient rest = RestClient.create();
        String response = rest.post()
                .uri("http://localhost:" + port + "/publish/orders")
                .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                .body("{\"payload\":\"first-order\"}")
                .retrieve()
                .body(String.class);
        assertNotNull(response);

        assertTrue(received.await(5, TimeUnit.SECONDS), "should receive the message over WebSocket");
        String json = messages.get(0);
        assertEquals("first-order", new String(Base64.getDecoder().decode(jsonField(json, "payload"))));
        String id = jsonField(json, "id");
        assertFalse(id.isEmpty());

        session.sendMessage(new TextMessage("ACK " + id));

        await(() -> queueManager.getQueue("orders").inFlightCount() == 0
                && queueManager.getQueue("orders").size() == 0, 3_000);
    }

    @Test
    void competingConsumersInSameGroupShareMessages() throws Exception {
        List<String> aMessages = new CopyOnWriteArrayList<>();
        List<String> bMessages = new CopyOnWriteArrayList<>();
        CountDownLatch total = new CountDownLatch(4);

        openSessions.add(connect("jobs", "workers", total, aMessages));
        openSessions.add(connect("jobs", "workers", total, bMessages));

        RestClient rest = RestClient.create();
        for (int i = 0; i < 4; i++) {
            rest.post()
                    .uri("http://localhost:" + port + "/publish/jobs")
                    .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                    .body("{\"payload\":\"job-" + i + "\"}")
                    .retrieve()
                    .body(String.class);
        }

        assertTrue(total.await(5, TimeUnit.SECONDS), "all 4 messages should be delivered");
        assertEquals(4, aMessages.size() + bMessages.size());
        assertTrue(aMessages.size() > 0 && bMessages.size() > 0,
                "both workers should get a share (a=" + aMessages.size() + ", b=" + bMessages.size() + ")");
    }

    private WebSocketSession connect(String topic, String group, CountDownLatch latch, List<String> target)
            throws Exception {
        StandardWebSocketClient client = new StandardWebSocketClient();
        StringBuilder uri = new StringBuilder("ws://localhost:" + port + "/subscribe/" + topic);
        if (group != null) uri.append("?group=").append(group);

        TextWebSocketHandler handler = new TextWebSocketHandler() {
            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) {
                target.add(message.getPayload());
                latch.countDown();
            }
        };
        CompletableFuture<WebSocketSession> future = client.execute(handler, uri.toString());
        return future.get(5, TimeUnit.SECONDS);
    }

    private static void await(java.util.function.BooleanSupplier condition, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(50);
        }
        fail("condition not met within " + timeoutMillis + " ms");
    }

    private static String jsonField(String json, String field) {
        String key = "\"" + field + "\":\"";
        int start = json.indexOf(key);
        if (start < 0) return "";
        int valueStart = start + key.length();
        int valueEnd = json.indexOf('"', valueStart);
        return json.substring(valueStart, valueEnd);
    }
}
