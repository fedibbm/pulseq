package com.pulseq.server;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Cluster settings, bound from {@code pulseq.cluster.*}. With {@code enabled=false} (the
 * default) the server runs alone and nothing here is used.
 *
 * <p>{@code nodes} lists every broker of the cluster, this one included, as
 * {@code id@clusterHost:clusterPort@publicUrl} separated by commas, for example
 * {@code 0@10.0.0.1:9100@http://10.0.0.1:8080,1@10.0.0.2:9100@http://10.0.0.2:8080}.
 * The cluster address carries traffic between brokers; the public URL is where clients are
 * sent when they reach a broker that is not the leader. Every broker must be given the same
 * list.</p>
 */
@ConfigurationProperties(prefix = "pulseq.cluster")
public class ClusterProperties {

    /** One broker of the cluster. */
    public record Node(int id, InetSocketAddress clusterAddress, String publicUrl) {
    }

    private boolean enabled;
    private int nodeId;
    private String nodes = "";
    private String dataDir = "./data";
    private long commitTimeoutMs = 5_000;
    private long tickMs = 10;
    private int electionMinTicks = 50;
    private int electionMaxTicks = 100;
    private int heartbeatTicks = 10;

    /** Parses {@link #getNodes()}; fails with a message naming the bad entry. */
    public Map<Integer, Node> parseNodes() {
        Map<Integer, Node> parsed = new LinkedHashMap<>();
        for (String entry : nodes.split(",")) {
            if (entry.isBlank()) {
                continue;
            }
            String[] parts = entry.trim().split("@", 3);
            try {
                int id = Integer.parseInt(parts[0].trim());
                String[] hostPort = parts[1].trim().split(":");
                InetSocketAddress address = new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1]));
                String url = parts[2].trim().replaceAll("/+$", "");
                if (url.isEmpty() || parsed.put(id, new Node(id, address, url)) != null) {
                    throw new IllegalArgumentException("empty URL or repeated id");
                }
            } catch (RuntimeException e) {
                throw new IllegalStateException("pulseq.cluster.nodes: cannot read '" + entry.trim()
                        + "', expected id@host:port@publicUrl", e);
            }
        }
        if (!parsed.containsKey(nodeId)) {
            throw new IllegalStateException("pulseq.cluster.nodes has no entry for this broker, "
                    + "pulseq.cluster.node-id=" + nodeId);
        }
        return parsed;
    }

    /** Public URL of a broker, if this server is clustered and knows the id. */
    public Optional<String> publicUrlOf(int id) {
        if (!enabled) {
            return Optional.empty();
        }
        return Optional.ofNullable(parseNodes().get(id)).map(Node::publicUrl);
    }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public int getNodeId() { return nodeId; }
    public void setNodeId(int nodeId) { this.nodeId = nodeId; }

    public String getNodes() { return nodes; }
    public void setNodes(String nodes) { this.nodes = nodes; }

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }

    public long getCommitTimeoutMs() { return commitTimeoutMs; }
    public void setCommitTimeoutMs(long commitTimeoutMs) { this.commitTimeoutMs = commitTimeoutMs; }

    public long getTickMs() { return tickMs; }
    public void setTickMs(long tickMs) { this.tickMs = tickMs; }

    public int getElectionMinTicks() { return electionMinTicks; }
    public void setElectionMinTicks(int electionMinTicks) { this.electionMinTicks = electionMinTicks; }

    public int getElectionMaxTicks() { return electionMaxTicks; }
    public void setElectionMaxTicks(int electionMaxTicks) { this.electionMaxTicks = electionMaxTicks; }

    public int getHeartbeatTicks() { return heartbeatTicks; }
    public void setHeartbeatTicks(int heartbeatTicks) { this.heartbeatTicks = heartbeatTicks; }
}
