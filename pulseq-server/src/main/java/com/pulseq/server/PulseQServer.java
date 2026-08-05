package com.pulseq.server;

import com.pulseq.core.Dispatcher;
import com.pulseq.core.InMemoryMessageStore;
import com.pulseq.core.MessageStore;
import com.pulseq.core.PostgresMessageStore;
import com.pulseq.core.QueueManager;
import com.pulseq.core.RetentionSweeper;
import com.pulseq.core.VisibilityTimeoutChecker;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot entry point for the PulseQ broker.
 *
 * <p>The core broker is framework-free; this module only wires it up, exposing a thin REST
 * API for publishing and a WebSocket endpoint for consuming.</p>
 */
@SpringBootApplication
@EnableConfigurationProperties(BrokerConfigProperties.class)
public class PulseQServer {

    public static void main(String[] args) {
        SpringApplication.run(PulseQServer.class, args);
    }

    @Bean
    MessageStore messageStore(BrokerConfigProperties properties) {
        if ("postgres".equalsIgnoreCase(properties.getStore())) {
            if (properties.getPostgresUrl() == null || properties.getPostgresUrl().isBlank()) {
                throw new IllegalStateException("pulseq.postgres-url is required when pulseq.store=postgres");
            }
            return new PostgresMessageStore(properties.getPostgresUrl(),
                    properties.getPostgresUser(), properties.getPostgresPassword());
        }
        return new InMemoryMessageStore();
    }

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
}
