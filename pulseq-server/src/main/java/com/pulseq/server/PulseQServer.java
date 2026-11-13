package com.pulseq.server;

import com.pulseq.core.Broker;
import com.pulseq.core.ClusterConfig;
import com.pulseq.core.ClusteredBroker;
import com.pulseq.core.Dispatcher;
import com.pulseq.core.InMemoryMessageStore;
import com.pulseq.core.MessageStore;
import com.pulseq.core.PostgresMessageStore;
import com.pulseq.core.QueueManager;
import com.pulseq.core.RetentionSweeper;
import com.pulseq.core.StandaloneBroker;
import com.pulseq.core.VisibilityTimeoutChecker;
import com.pulseq.raft.FileRaftStorage;
import com.pulseq.raft.RaftConfig;
import com.pulseq.raft.TcpTransport;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Spring Boot entry point for the PulseQ broker.
 *
 * <p>The core broker is framework-free; this module only wires it up, exposing a thin REST
 * API for publishing and a WebSocket endpoint for consuming. With
 * {@code pulseq.cluster.enabled=true} the server is one broker of a cluster and serves
 * clients only while it is the leader.</p>
 */
@SpringBootApplication
@EnableConfigurationProperties({BrokerConfigProperties.class, ClusterProperties.class})
public class PulseQServer {

    public static void main(String[] args) {
        SpringApplication.run(PulseQServer.class, args);
    }

    /** In a cluster this is the broker's own copy of the store, written only through Raft. */
    @Bean(destroyMethod = "close")
    MessageStore messageStore(BrokerConfigProperties properties) {
        if ("postgres".equalsIgnoreCase(properties.getStore())) {
            if (properties.getPostgresUrl() == null || properties.getPostgresUrl().isBlank()) {
                throw new IllegalStateException("pulseq.postgres-url is required when pulseq.store=postgres");
            }
            return new PostgresMessageStore(properties.getPostgresUrl(),
                    properties.getPostgresUser(), properties.getPostgresPassword(),
                    properties.getStorePoolSize());
        }
        return new InMemoryMessageStore();
    }

    /** A single broker: the queues live for as long as the process. */
    @Configuration
    @ConditionalOnProperty(name = "pulseq.cluster.enabled", havingValue = "false", matchIfMissing = true)
    static class Standalone {

        @Bean
        QueueManager queueManager(MessageStore store, BrokerConfigProperties properties) {
            QueueManager queueManager = new QueueManager(store, properties.toBrokerConfig());
            queueManager.recover();
            return queueManager;
        }

        @Bean(destroyMethod = "shutdown")
        Dispatcher dispatcher(QueueManager queueManager, BrokerConfigProperties properties) {
            return new Dispatcher(queueManager, properties.getConsumerThreads());
        }

        @Bean(destroyMethod = "stop")
        VisibilityTimeoutChecker checker(QueueManager queueManager, BrokerConfigProperties properties) {
            VisibilityTimeoutChecker checker =
                    new VisibilityTimeoutChecker(queueManager, properties.getTimeoutCheckRateMillis());
            checker.start();
            return checker;
        }

        @Bean(destroyMethod = "stop")
        RetentionSweeper retentionSweeper(MessageStore store, BrokerConfigProperties properties) {
            RetentionSweeper sweeper =
                    new RetentionSweeper(store, properties.getRetentionMillis(), properties.getRetentionSweepRateMillis());
            sweeper.start();
            return sweeper;
        }

        @Bean
        Broker broker(QueueManager queueManager, Dispatcher dispatcher) {
            return new StandaloneBroker(queueManager, dispatcher);
        }
    }

    /** One broker of a cluster: Raft log on disk, TCP to the other brokers. */
    @Configuration
    @ConditionalOnProperty(name = "pulseq.cluster.enabled", havingValue = "true")
    static class Clustered {

        @Bean(destroyMethod = "close")
        FileRaftStorage raftStorage(ClusterProperties cluster) {
            return new FileRaftStorage(Path.of(cluster.getDataDir(), "raft"));
        }

        @Bean(destroyMethod = "close")
        TcpTransport raftTransport(ClusterProperties cluster) {
            Map<Integer, InetSocketAddress> addresses = new HashMap<>();
            cluster.parseNodes().forEach((id, node) -> addresses.put(id, node.clusterAddress()));
            return new TcpTransport(cluster.getNodeId(), addresses);
        }

        @Bean(destroyMethod = "close")
        ClusteredBroker broker(MessageStore store, FileRaftStorage raftStorage, TcpTransport transport,
                               BrokerConfigProperties properties, ClusterProperties cluster) {
            RaftConfig raft = new RaftConfig(cluster.getElectionMinTicks(), cluster.getElectionMaxTicks(),
                    cluster.getHeartbeatTicks(), 64);
            ClusteredBroker broker = new ClusteredBroker(cluster.getNodeId(),
                    List.copyOf(cluster.parseNodes().keySet()), store, raftStorage, transport,
                    properties.toBrokerConfig(),
                    new ClusterConfig(raft, cluster.getTickMs(), cluster.getCommitTimeoutMs()));
            if (properties.getRetentionMillis() > 0) {
                broker.enableRetention(properties.getRetentionMillis(), properties.getRetentionSweepRateMillis());
            }
            transport.start(broker.raft()::receive);
            broker.start();
            return broker;
        }
    }
}
