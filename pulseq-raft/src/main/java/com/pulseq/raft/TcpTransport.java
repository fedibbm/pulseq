package com.pulseq.raft;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Sends Raft messages between processes over TCP. Each node listens on one port and keeps one
 * outgoing connection per peer, opened on demand and reopened after a failure.
 *
 * <p>Delivery is best effort. While a peer is unreachable its messages are dropped, never
 * queued up: Raft resends what matters, and an old message is worth nothing by the time the
 * peer returns.</p>
 *
 * <p>Connections are neither authenticated nor encrypted, so the cluster port must only be
 * reachable by the other brokers.</p>
 */
public final class TcpTransport implements Transport, AutoCloseable {

    private static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;
    private static final int CONNECT_TIMEOUT_MILLIS = 500;
    private static final int RETRY_MILLIS = 100;
    private static final int QUEUE_CAPACITY = 1024;

    private final int id;
    private final Map<Integer, InetSocketAddress> addresses;
    private final Map<Integer, Link> links = new ConcurrentHashMap<>();
    private final Set<Socket> inbound = ConcurrentHashMap.newKeySet();
    private volatile ServerSocket server;
    private volatile boolean closed;

    /**
     * @param id        this node's id
     * @param addresses cluster address of every node, including this one
     */
    public TcpTransport(int id, Map<Integer, InetSocketAddress> addresses) {
        if (!addresses.containsKey(id)) {
            throw new IllegalArgumentException("no address for node " + id);
        }
        this.id = id;
        this.addresses = Map.copyOf(addresses);
    }

    /** Starts listening and passes every message received to {@code receiver}. */
    public void start(Consumer<Envelope> receiver) {
        try {
            ServerSocket socket = new ServerSocket();
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(addresses.get(id).getPort()));
            server = socket;
        } catch (IOException e) {
            throw new UncheckedIOException("node " + id + " cannot listen on " + addresses.get(id), e);
        }
        daemon("pulseq-raft-accept-" + id, () -> accept(receiver));
        for (int peer : addresses.keySet()) {
            if (peer != id) {
                Link link = new Link(peer);
                links.put(peer, link);
                daemon("pulseq-raft-send-" + id + "-to-" + peer, link::run);
            }
        }
    }

    @Override
    public void send(Envelope envelope) {
        Link link = links.get(envelope.to());
        if (link != null) {
            link.queue.offer(RaftCodec.encode(envelope)); // full queue: drop, Raft resends
        }
    }

    // ------------------------------------------------------------------ incoming

    private void accept(Consumer<Envelope> receiver) {
        while (!closed) {
            try {
                Socket socket = server.accept();
                socket.setTcpNoDelay(true);
                inbound.add(socket);
                daemon("pulseq-raft-receive-" + id, () -> receive(socket, receiver));
            } catch (IOException e) {
                if (!closed) {
                    pause();
                }
            }
        }
    }

    private void receive(Socket socket, Consumer<Envelope> receiver) {
        try (socket; DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()))) {
            while (!closed) {
                int length = in.readInt();
                if (length <= 0 || length > MAX_FRAME_BYTES) {
                    return; // not a peer speaking this protocol
                }
                byte[] frame = in.readNBytes(length);
                if (frame.length != length) {
                    return;
                }
                Envelope envelope = RaftCodec.decodeEnvelope(frame);
                if (envelope.to() == id && addresses.containsKey(envelope.from())) {
                    receiver.accept(envelope);
                }
            }
        } catch (IOException | UncheckedIOException e) {
            // The peer went away or sent garbage; it reconnects if it is still a peer.
        } finally {
            inbound.remove(socket);
        }
    }

    // ------------------------------------------------------------------ outgoing

    private final class Link {
        private final int peer;
        private final BlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        private volatile Socket socket;

        Link(int peer) {
            this.peer = peer;
        }

        void run() {
            while (!closed) {
                try (Socket connection = new Socket()) {
                    connection.setTcpNoDelay(true);
                    connection.connect(addresses.get(peer), CONNECT_TIMEOUT_MILLIS);
                    socket = connection;
                    queue.clear(); // whatever piled up while disconnected is stale
                    DataOutputStream out = new DataOutputStream(
                            new BufferedOutputStream(connection.getOutputStream()));
                    while (!closed) {
                        byte[] frame = queue.take();
                        out.writeInt(frame.length);
                        out.write(frame);
                        if (queue.isEmpty()) {
                            out.flush();
                        }
                    }
                } catch (IOException e) {
                    queue.clear();
                    pause();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        void close() {
            Socket current = socket;
            if (current != null) {
                closeQuietly(current);
            }
        }
    }

    // ------------------------------------------------------------------ lifecycle

    @Override
    public void close() {
        closed = true;
        ServerSocket socket = server;
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // closing anyway
            }
        }
        links.values().forEach(Link::close);
        inbound.forEach(TcpTransport::closeQuietly);
        threads.forEach(Thread::interrupt);
    }

    private final Set<Thread> threads = ConcurrentHashMap.newKeySet();

    private void daemon(String name, Runnable body) {
        Thread thread = new Thread(() -> {
            try {
                body.run();
            } finally {
                threads.remove(Thread.currentThread());
            }
        }, name);
        thread.setDaemon(true);
        threads.add(thread);
        thread.start();
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // closing anyway
        }
    }

    private void pause() {
        try {
            Thread.sleep(RETRY_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
