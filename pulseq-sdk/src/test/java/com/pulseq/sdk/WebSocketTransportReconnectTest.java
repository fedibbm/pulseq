package com.pulseq.sdk;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exercises {@link WebSocketTransport} over a real socket.
 *
 * <p>{@code ReconnectPolicyTest} covers the backoff arithmetic in isolation. Correct
 * arithmetic does not prove the transport <em>uses</em> it: a client can compute the right
 * delay and still never reconnect if {@code onClose} is not wired to
 * {@code scheduleReconnect}. These tests drive the real transport against a minimal
 * hand-rolled WebSocket endpoint, so a connection can be severed on demand and recovery
 * observed directly.</p>
 *
 * <p>The endpoint is a raw {@link ServerSocket} rather than a framework, because the
 * behaviour under test is precisely the low-level connect/close path: it performs the
 * RFC 6455 handshake, sends unmasked text frames, reads masked client frames, and can drop
 * every connection instantly.</p>
 */
class WebSocketTransportReconnectTest {

    private static final String GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    private ServerSocket serverSocket;
    private Thread acceptor;
    private volatile boolean running = true;
    private String baseUrl;

    private final List<Socket> openSockets = new CopyOnWriteArrayList<>();
    private final AtomicInteger connectionCount = new AtomicInteger();

    @BeforeEach
    void startEndpoint() throws IOException {
        serverSocket = new ServerSocket(0, 50, java.net.InetAddress.getByName("127.0.0.1"));
        baseUrl = "http://127.0.0.1:" + serverSocket.getLocalPort();

        acceptor = new Thread(this::acceptLoop, "test-ws-acceptor");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    @AfterEach
    void stopEndpoint() {
        running = false;
        severConnections();
        try {
            serverSocket.close();
        } catch (IOException ignore) {
            // shutting down
        }
    }

    // ---------------------------------------------------------------- endpoint

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                openSockets.add(socket);
                connectionCount.incrementAndGet();
                Thread worker = new Thread(() -> serve(socket), "test-ws-session");
                worker.setDaemon(true);
                worker.start();
            } catch (IOException e) {
                if (running) {
                    // socket closed during shutdown
                }
                return;
            }
        }
    }

    private void serve(Socket socket) {
        try {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            String key = readHandshake(in);
            if (key == null) {
                return;
            }
            String response = "HTTP/1.1 101 Switching Protocols\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Accept: " + base64Sha1(key + GUID) + "\r\n\r\n";
            out.write(response.getBytes(StandardCharsets.US_ASCII));
            out.flush();

            // Reader thread: drain masked client frames so the socket stays healthy.
            Thread reader = new Thread(() -> drain(in), "test-ws-reader");
            reader.setDaemon(true);
            reader.start();

            // Hold the connection open until the test severs it.
            while (running && !socket.isClosed() && openSockets.contains(socket)) {
                Thread.sleep(25);
            }
        } catch (Exception e) {
            // connection ended; the client is expected to reconnect
        } finally {
            closeQuietly(socket);
        }
    }

    /** Reads request lines until the blank line, returning Sec-WebSocket-Key. */
    private String readHandshake(InputStream in) throws IOException {
        String key = null;
        String line;
        while ((line = readLine(in)) != null && !line.isEmpty()) {
            int colon = line.indexOf(':');
            if (colon > 0 && line.substring(0, colon).trim().equalsIgnoreCase("Sec-WebSocket-Key")) {
                key = line.substring(colon + 1).trim();
            }
        }
        return key;
    }

    private String readLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) >= 0) {
            if (c == '\r') {
                in.read(); // consume '\n'
                return sb.toString();
            }
            sb.append((char) c);
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    private void drain(InputStream in) {
        try {
            while (in.read() >= 0) {
                // Client frames are masked; their content is not asserted here.
            }
        } catch (IOException ignore) {
            // connection closed
        }
    }

    /** Sends a delivery frame shaped like the server's JSON frame. */
    private void deliver(Socket socket, String messageId, String payloadB64, int attempts) {
        String json = "{\"id\":\"" + messageId + "\",\"topic\":\"t\",\"deliveryAttempts\":"
                + attempts + ",\"payload\":\"" + payloadB64 + "\"}";
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        try {
            OutputStream out = socket.getOutputStream();
            synchronized (out) {
                out.write(0x81); // FIN + text
                if (bytes.length < 126) {
                    out.write(bytes.length);
                } else {
                    out.write(126);
                    out.write((bytes.length >> 8) & 0xFF);
                    out.write(bytes.length & 0xFF);
                }
                out.write(bytes);
                out.flush();
            }
        } catch (IOException e) {
            // socket already gone; the client will reconnect
        }
    }

    /** Severs every open connection, as a server restart or network drop would. */
    private void severConnections() {
        List<Socket> sockets = new ArrayList<>(openSockets);
        openSockets.removeAll(sockets);
        sockets.forEach(WebSocketTransportReconnectTest::closeQuietly);
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignore) {
            // best effort
        }
    }

    private static String base64Sha1(String value) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            return Base64.getEncoder().encodeToString(sha1.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private Socket awaitSocket(int index, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            List<Socket> sockets = new ArrayList<>(openSockets);
            if (sockets.size() > index) {
                return sockets.get(index);
            }
            Thread.sleep(25);
        }
        fail("no socket at index " + index + " within " + timeoutMillis + " ms");
        return null;
    }

    private void waitFor(BooleanSupplier condition, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        fail("condition not met within " + timeoutMillis + " ms");
    }

    // ------------------------------------------------------------------ tests

    @Test
    void deliversMessagesReceivedOverTheSocket() throws Exception {
        WebSocketTransport transport = new WebSocketTransport(baseUrl, new ReconnectPolicy(50, 200));
        try {
            List<String> received = new CopyOnWriteArrayList<>();
            transport.subscribe("t", message -> received.add(message.getId()));

            Socket socket = awaitSocket(0, 5_000);
            deliver(socket, "m-1",
                    Base64.getEncoder().encodeToString("hello".getBytes(StandardCharsets.UTF_8)), 1);

            waitFor(() -> received.contains("m-1"), 5_000);
        } finally {
            transport.close();
        }
    }

    @Test
    void reconnectsAndResumesAfterTheConnectionDrops() throws Exception {
        WebSocketTransport transport = new WebSocketTransport(baseUrl, new ReconnectPolicy(50, 500));
        try {
            List<String> received = new CopyOnWriteArrayList<>();
            transport.subscribe("t", message -> received.add(message.getId()));

            Socket first = awaitSocket(0, 5_000);
            deliver(first, "before-drop",
                    Base64.getEncoder().encodeToString("one".getBytes(StandardCharsets.UTF_8)), 1);
            waitFor(() -> received.contains("before-drop"), 5_000);

            // The connection dies. onClose must schedule a reconnect, which shows up as a
            // second handshake.
            severConnections();
            waitFor(() -> connectionCount.get() >= 2, 10_000);

            // And the resumed subscription must actually deliver again.
            Socket reconnected = awaitSocket(0, 5_000);
            deliver(reconnected, "after-reconnect",
                    Base64.getEncoder().encodeToString("two".getBytes(StandardCharsets.UTF_8)), 1);
            waitFor(() -> received.contains("after-reconnect"), 5_000);
        } finally {
            transport.close();
        }
    }

    @Test
    void reconnectsRepeatedlyAcrossSeveralDrops() throws Exception {
        WebSocketTransport transport = new WebSocketTransport(baseUrl, new ReconnectPolicy(25, 200));
        try {
            transport.subscribe("t", message -> { });
            awaitSocket(0, 5_000);

            for (int index = 1; index <= 3; index++) {
                severConnections();
                int expected = index + 1;
                waitFor(() -> connectionCount.get() >= expected, 10_000);
            }
            assertTrue(connectionCount.get() >= 4,
                    "the transport must recover from repeated drops, saw " + connectionCount.get());
        } finally {
            transport.close();
        }
    }

    @Test
    void closeStopsReconnecting() throws Exception {
        WebSocketTransport transport = new WebSocketTransport(baseUrl, new ReconnectPolicy(25, 200));
        transport.subscribe("t", message -> { });
        awaitSocket(0, 5_000);

        transport.close();
        int afterClose = connectionCount.get();

        // Sever whatever is still open; a closed transport must not open a new connection.
        severConnections();
        Thread.sleep(700);

        assertEquals(afterClose, connectionCount.get(),
                "a closed transport must not reconnect");
    }

    @Test
    void keepsRetryingWhileTheEndpointIsUnreachable() throws Exception {
        // Nothing is listening on this port: the first connect fails and the transport must
        // keep retrying rather than give up or spin.
        int deadPort;
        try (ServerSocket probe = new ServerSocket(0)) {
            deadPort = probe.getLocalPort();
        }

        WebSocketTransport transport =
                new WebSocketTransport("http://127.0.0.1:" + deadPort, new ReconnectPolicy(25, 100));
        try {
            transport.subscribe("t", message -> { });
            Thread.sleep(400);
            // Nothing to assert beyond reaching close() promptly: repeated failed connects
            // must neither hang nor busy-spin.
        } finally {
            transport.close();
        }
    }
}