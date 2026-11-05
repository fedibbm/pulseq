package com.pulseq.raft;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Connects the nodes of one JVM directly, for embedded clusters and tests. A node can be
 * disconnected to simulate a network failure: messages to and from it are dropped.
 */
public final class LocalNetwork {

    private final Map<Integer, RaftRuntime> nodes = new ConcurrentHashMap<>();
    private final Set<Integer> disconnected = ConcurrentHashMap.newKeySet();

    /** The transport a node uses to send. Every node of the cluster shares this network. */
    public Transport transport() {
        return envelope -> {
            if (disconnected.contains(envelope.from()) || disconnected.contains(envelope.to())) {
                return;
            }
            RaftRuntime target = nodes.get(envelope.to());
            if (target != null) {
                target.receive(envelope);
            }
        };
    }

    public void register(RaftRuntime node) {
        nodes.put(node.id(), node);
    }

    public void unregister(int id) {
        nodes.remove(id);
    }

    public void disconnect(int id) {
        disconnected.add(id);
    }

    public void reconnect(int id) {
        disconnected.remove(id);
    }
}
