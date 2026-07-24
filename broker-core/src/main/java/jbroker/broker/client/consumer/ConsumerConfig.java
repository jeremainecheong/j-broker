package jbroker.broker.client.consumer;

import java.time.Duration;
import jbroker.tls.TlsConfig;

/**
 * Immutable {@link Consumer} configuration. Use the {@link Builder} to
 * construct — chained setters with named parameters keep the call site
 * readable when tuning many fields.
 */
public final class ConsumerConfig {

    public static final int DEFAULT_SESSION_TIMEOUT_MS = 45_000;
    public static final int DEFAULT_REBALANCE_TIMEOUT_MS = 60_000;
    public static final int DEFAULT_FETCH_MAX_BYTES = 1 << 20; // 1 MiB
    public static final int DEFAULT_MAX_POLL_RECORDS = 500;

    private final String bootstrapHost;
    private final int bootstrapPort;
    private final String groupId;
    private final String instanceId;
    private final int sessionTimeoutMs;
    private final int rebalanceTimeoutMs;
    private final int fetchMaxBytes;
    private final int maxPollRecords;
    private final Duration pollFetchDeadline;
    private final DeadLetterPolicy deadLetterPolicy;
    private final TlsConfig tls;

    private ConsumerConfig(Builder b) {
        this.bootstrapHost = b.bootstrapHost;
        this.bootstrapPort = b.bootstrapPort;
        this.groupId = b.groupId;
        this.instanceId = b.instanceId;
        this.sessionTimeoutMs = b.sessionTimeoutMs;
        this.rebalanceTimeoutMs = b.rebalanceTimeoutMs;
        this.fetchMaxBytes = b.fetchMaxBytes;
        this.maxPollRecords = b.maxPollRecords;
        this.pollFetchDeadline = b.pollFetchDeadline;
        this.deadLetterPolicy = b.deadLetterPolicy;
        this.tls = b.tls == null ? TlsConfig.DISABLED : b.tls;
    }

    /** TLS / mTLS configuration; defaults to {@link TlsConfig#DISABLED} (plaintext). */
    public TlsConfig tls() {
        return tls;
    }

    public String bootstrapHost() {
        return bootstrapHost;
    }

    public int bootstrapPort() {
        return bootstrapPort;
    }

    public String groupId() {
        return groupId;
    }

    public String instanceId() {
        return instanceId;
    }

    public int sessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public int rebalanceTimeoutMs() {
        return rebalanceTimeoutMs;
    }

    public int fetchMaxBytes() {
        return fetchMaxBytes;
    }

    public int maxPollRecords() {
        return maxPollRecords;
    }

    public Duration pollFetchDeadline() {
        return pollFetchDeadline;
    }

    /** Dead-letter routing policy, or {@code null} if disabled. */
    public DeadLetterPolicy deadLetterPolicy() {
        return deadLetterPolicy;
    }

    public static Builder builder(String groupId, String bootstrapHost, int bootstrapPort) {
        return new Builder(groupId, bootstrapHost, bootstrapPort);
    }

    public static final class Builder {
        private final String groupId;
        private final String bootstrapHost;
        private final int bootstrapPort;
        private String instanceId = "";
        private int sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT_MS;
        private int rebalanceTimeoutMs = DEFAULT_REBALANCE_TIMEOUT_MS;
        private int fetchMaxBytes = DEFAULT_FETCH_MAX_BYTES;
        private int maxPollRecords = DEFAULT_MAX_POLL_RECORDS;
        private Duration pollFetchDeadline = Duration.ofSeconds(5);
        private DeadLetterPolicy deadLetterPolicy;
        private TlsConfig tls;

        private Builder(String groupId, String bootstrapHost, int bootstrapPort) {
            this.groupId = groupId;
            this.bootstrapHost = bootstrapHost;
            this.bootstrapPort = bootstrapPort;
        }

        /** Static membership: same instance_id rejoining preserves the slot. Empty for dynamic. */
        public Builder instanceId(String s) {
            this.instanceId = s == null ? "" : s;
            return this;
        }

        public Builder sessionTimeoutMs(int v) {
            this.sessionTimeoutMs = v;
            return this;
        }

        public Builder rebalanceTimeoutMs(int v) {
            this.rebalanceTimeoutMs = v;
            return this;
        }

        public Builder fetchMaxBytes(int v) {
            this.fetchMaxBytes = v;
            return this;
        }

        /**
         * Upper bound on records a single {@code poll} returns. Surplus
         * records already fetched stay buffered client-side, in order, for
         * the next poll — never dropped.
         */
        public Builder maxPollRecords(int v) {
            if (v <= 0) throw new IllegalArgumentException("max.poll.records must be positive: " + v);
            this.maxPollRecords = v;
            return this;
        }

        public Builder pollFetchDeadline(Duration d) {
            this.pollFetchDeadline = d;
            return this;
        }

        /**
         * Enable dead-letter routing for handler-driven {@code poll} calls.
         * Pass {@code null} to disable (the default).
         */
        public Builder deadLetterPolicy(DeadLetterPolicy p) {
            this.deadLetterPolicy = p;
            return this;
        }

        /** Supply a TLS config; defaults to {@link TlsConfig#DISABLED} (plaintext). */
        public Builder tls(TlsConfig t) {
            this.tls = t;
            return this;
        }

        public ConsumerConfig build() {
            return new ConsumerConfig(this);
        }
    }
}
