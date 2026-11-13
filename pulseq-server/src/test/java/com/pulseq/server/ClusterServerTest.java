package com.pulseq.server;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Three PulseQ servers in cluster mode, driven only through their HTTP and WebSocket APIs. */
class ClusterServerTest {

    private static final Pattern MESSAGE_ID = Pattern.compile("\"id\":\"([^\"]+)\"");

    @TempDir
    Path directory;

    private final Map<Integer, Integer> httpPorts = new HashMap<>();
    private final Map<Integer, ConfigurableApplicationContext> servers = new HashMap<>();
    private final List<WebSocket> sockets = new ArrayList<>();
    private final Set<String> received = new ConcurrentSkipListSet<>();
    private final HttpClient plain = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final HttpClient following = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2))
            .followRedirects(HttpClient.Redirect.NORMAL).build();
    private String nodes;

    @BeforeEach
    void startCluster() throws IOException {
        StringBuilder list = new StringBuilder();
        for (int id = 0; id < 3; id++) {
            int http = freePort();
            httpPorts.put(id, http);
            list.append(id == 0 ? "" : ",").append(id).append("@127.0.0.1:").append(freePort())
                    .append("@http://127.0.0.1:").append(http);
        }
        nodes = list.toString();
        for (int id = 0; id < 3; id++) {
            servers.put(id, new SpringApplicationBuilder(PulseQServer.class).run(
                    "--server.port=" + httpPorts.get(id),
                    "--pulseq.cluster.enabled=true",
                    "--pulseq.cluster.node-id=" + id,
                    "--pulseq.cluster.nodes=" + nodes,
                    "--pulseq.cluster.data-dir=" + directory.resolve("node-" + id),
                    "--pulseq.visibility-timeout-ms=2000",
                    "--pulseq.timeout-check-rate-ms=100",
                    "--pulseq.dashboard-path=" + directory.resolve("no-dashboard"),
                    "--spring.main.banner-mode=off",
                    "--logging.level.root=WARN"));
        }
    }

    @AfterEach
    void stopCluster() {
        sockets.forEach(WebSocket::abort);
        servers.values().forEach(ConfigurableApplicationContext::close);
    }

    @Test
    void followerSendsPublishersToTheLeader() throws Exception {
        int leader = awaitLeader();
        int follower = (leader + 1) % 3;
        await("the follower to learn who leads", () -> get(follower, "/health").body().contains("\"leaderId\":" + leader));

        HttpResponse<String> redirect = publish(plain, follower, "a1");
        assertEquals(307, redirect.statusCode());
        assertEquals(url(leader) + "/publish/orders",
                redirect.headers().firstValue("Location").orElseThrow());

        assertEquals(200, publish(following, follower, "a1").statusCode(), "the redirect leads to an accepted publish");
        assertEquals(409, publish(following, follower, "a1").statusCode(), "the same id again is a duplicate");

        String followerHealth = get(follower, "/health").body();
        assertTrue(followerHealth.contains("\"role\":\"FOLLOWER\""), followerHealth);
        assertTrue(followerHealth.contains("\"leaderUrl\":\"" + url(leader) + "\""), followerHealth);
        String leaderHealth = get(leader, "/health").body();
        assertTrue(leaderHealth.contains("\"role\":\"LEADER\""), leaderHealth);
        assertTrue(leaderHealth.contains("\"orders\":1"), leaderHealth);
        assertEquals(503, get(follower, "/health/leader").statusCode());
        assertEquals(200, get(leader, "/health/leader").statusCode());
    }

    @Test
    void consumersReceiveEverythingAcrossALeaderStop() throws Exception {
        int first = awaitLeader();
        for (int id = 0; id < 3; id++) {
            connectConsumer(id); // one consumer per broker, same group: only the leader feeds its own
        }
        Set<String> published = new TreeSet<>();
        for (int i = 0; i < 20; i++) {
            published.add(publishUntilAccepted("m" + i, (first + 1) % 3));
        }

        servers.remove(first).close(); // the leader's whole server goes away

        int survivor = servers.keySet().iterator().next();
        for (int i = 20; i < 40; i++) {
            published.add(publishUntilAccepted("m" + i, survivor));
        }
        assertTrue(awaitLeader() != first);
        await("every published message to reach a consumer", () -> received.containsAll(published));
    }

    // ------------------------------------------------------------------ helpers

    private String publishUntilAccepted(String id, int via) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try {
                int status = publish(following, via, id).statusCode();
                if (status == 200 || status == 409) {
                    return id; // stored now, or already stored by an earlier attempt
                }
            } catch (IOException unreachable) {
                // Redirected to a leader that just stopped: retry until a new one is known.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            pause();
        }
        throw new AssertionError("could not publish " + id);
    }

    private HttpResponse<String> publish(HttpClient client, int id, String messageId)
            throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url(id) + "/publish/orders"))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"payload\":\"hello\",\"messageId\":\"" + messageId + "\"}"))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(int id, String path) {
        try {
            return plain.send(HttpRequest.newBuilder(URI.create(url(id) + path))
                    .timeout(Duration.ofSeconds(5)).build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new AssertionError(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private void connectConsumer(int id) {
        WebSocket.Listener listener = new WebSocket.Listener() {
            private final StringBuilder text = new StringBuilder();

            @Override
            public CompletionStage<?> onText(WebSocket socket, CharSequence data, boolean last) {
                text.append(data);
                if (last) {
                    Matcher match = MESSAGE_ID.matcher(text);
                    if (match.find()) {
                        received.add(match.group(1));
                        socket.sendText("ACK " + match.group(1), true).join();
                    }
                    text.setLength(0);
                }
                socket.request(1);
                return null;
            }
        };
        sockets.add(plain.newWebSocketBuilder()
                .buildAsync(URI.create("ws://127.0.0.1:" + httpPorts.get(id) + "/subscribe/orders?group=workers"),
                        listener).join());
    }

    /** The one broker whose leader probe answers 200. */
    private int awaitLeader() {
        int[] leader = {-1};
        await("exactly one serving broker", () -> {
            List<Integer> serving = servers.keySet().stream()
                    .filter(id -> get(id, "/health/leader").statusCode() == 200).toList();
            if (serving.size() == 1) {
                leader[0] = serving.get(0);
            }
            return serving.size() == 1;
        });
        return leader[0];
    }

    private String url(int id) {
        return "http://127.0.0.1:" + httpPorts.get(id);
    }

    private static int freePort() throws IOException {
        try (ServerSocket probe = new ServerSocket(0)) {
            return probe.getLocalPort();
        }
    }

    private void await(String what, BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("gave up waiting for " + what);
            }
            pause();
        }
    }

    private static void pause() {
        try {
            Thread.sleep(20);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted");
        }
    }
}
