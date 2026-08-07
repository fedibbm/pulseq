package com.pulseq.server;

import com.pulseq.core.BrokerConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds the {@code pulseq.*} settings from application.yml into a {@link BrokerConfig}.
 */
@ConfigurationProperties(prefix = "pulseq")
public class BrokerConfigProperties {

    private int capacity = 1000;
    private long visibilityTimeoutMillis = 30_000;
    private long retryBaseDelayMillis = 500;
    private long maxRetryDelayMillis = 60_000;
    private int maxRetries = 3;
    private int consumerThreads = 8;
    private long timeoutCheckRateMillis = 1_000;
    private String store = "memory";
    private String postgresUrl;
    private String postgresUser;
    private String postgresPassword;
    private String dashboardPath = "../pulseq-dashboard/dist/pulseq-dashboard/browser";
    private double retentionHours = 24;
    private long retentionSweepRateMillis = 60_000;

    public BrokerConfig toBrokerConfig() {
        return new BrokerConfig(capacity, visibilityTimeoutMillis, retryBaseDelayMillis,
                maxRetryDelayMillis, maxRetries, consumerThreads, timeoutCheckRateMillis);
    }

    public int getCapacity() { return capacity; }
    public void setCapacity(int capacity) { this.capacity = capacity; }

    public long getVisibilityTimeoutMillis() { return visibilityTimeoutMillis; }
    public void setVisibilityTimeoutMillis(long visibilityTimeoutMillis) { this.visibilityTimeoutMillis = visibilityTimeoutMillis; }

    public long getRetryBaseDelayMillis() { return retryBaseDelayMillis; }
    public void setRetryBaseDelayMillis(long retryBaseDelayMillis) { this.retryBaseDelayMillis = retryBaseDelayMillis; }

    public long getMaxRetryDelayMillis() { return maxRetryDelayMillis; }
    public void setMaxRetryDelayMillis(long maxRetryDelayMillis) { this.maxRetryDelayMillis = maxRetryDelayMillis; }

    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }

    public int getConsumerThreads() { return consumerThreads; }
    public void setConsumerThreads(int consumerThreads) { this.consumerThreads = consumerThreads; }

    public long getTimeoutCheckRateMillis() { return timeoutCheckRateMillis; }
    public void setTimeoutCheckRateMillis(long timeoutCheckRateMillis) { this.timeoutCheckRateMillis = timeoutCheckRateMillis; }

    public String getStore() { return store; }
    public void setStore(String store) { this.store = store; }

    public String getPostgresUrl() { return postgresUrl; }
    public void setPostgresUrl(String postgresUrl) { this.postgresUrl = postgresUrl; }

    public String getPostgresUser() { return postgresUser; }
    public void setPostgresUser(String postgresUser) { this.postgresUser = postgresUser; }

    public String getPostgresPassword() { return postgresPassword; }
    public void setPostgresPassword(String postgresPassword) { this.postgresPassword = postgresPassword; }

    public String getDashboardPath() { return dashboardPath; }
    public void setDashboardPath(String dashboardPath) { this.dashboardPath = dashboardPath; }

    public double getRetentionHours() { return retentionHours; }
    public void setRetentionHours(double retentionHours) { this.retentionHours = retentionHours; }

    public long getRetentionSweepRateMillis() { return retentionSweepRateMillis; }
    public void setRetentionSweepRateMillis(long retentionSweepRateMillis) { this.retentionSweepRateMillis = retentionSweepRateMillis; }

    public long getRetentionMillis() {
        return (long) (retentionHours * 3_600_000);
    }
}
