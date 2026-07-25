package jbroker.app;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import jbroker.broker.AdminHandler;
import jbroker.broker.BrokerGrpcServices;
import jbroker.broker.ConsumerHandler;
import jbroker.broker.ConsumerOffsetsCreator;
import jbroker.broker.FetchHandler;
import jbroker.broker.InitProducerIdHandler;
import jbroker.broker.MetadataServiceHandler;
import jbroker.broker.MetadataStateMachine;
import jbroker.broker.OffsetsForLeaderEpochHandler;
import jbroker.broker.ProduceHandler;
import jbroker.broker.ProducerIdRegistry;
import jbroker.broker.ReplicaFetchHandler;
import jbroker.broker.TopicManager;
import jbroker.broker.WaitingStateMachine;
import jbroker.broker.auth.AuthMode;
import jbroker.broker.auth.MtlsAuthInterceptor;
import jbroker.broker.group.GroupCoordinator;
import jbroker.broker.group.OffsetCache;
import jbroker.broker.group.RangeAssignor;
import jbroker.broker.replication.FollowerStateTracker;
import jbroker.broker.replication.IsrManager;
import jbroker.raft.core.DefaultRaftCore;
import jbroker.raft.core.FilePersistentState;
import jbroker.raft.core.FileRaftLog;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.RaftConfig;
import jbroker.raft.transport.RaftDriver;
import jbroker.raft.transport.RaftPeerClient;
import jbroker.storage.LogManager;
import jbroker.tls.TlsConfig;
import jbroker.tls.TlsContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-node broker. Wires together:
 *
 * <ul>
 *   <li>A one-node Raft cluster (metadata only; data appends bypass Raft).</li>
 *   <li>{@link MetadataStateMachine} that replays {@code TopicRecord}s into
 *       a {@link TopicManager}.</li>
 *   <li>{@link LogManager} rooted at {@code dataDir/topics} for partition
 *       data logs.</li>
 *   <li>One gRPC server exposing Producer / Consumer / Admin services.</li>
 * </ul>
 *
 * <p>Bigger broker clusters replace the one-node Raft config with
 * a multi-node config; the data-plane stays the same shape.
 */
public final class Broker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Broker.class);

    public record Config(
            NodeId selfId,
            Path dataDir,
            int raftPort,
            int brokerPort,
            List<VoterAddress> voters,
            int consumerOffsetsPartitions,
            /** When ≥ 0, the broker binds a cooperative chaos HTTP server on this port. */
            int chaosPort,
            /** PreferredLeaderBalancer tick interval, ms. Default 15_000. */
            long balancerTickMillis,
            /** PreferredLeaderBalancer stability window, ms. Default 30_000. */
            long balancerStabilityMillis,
            /**
             * TLS configuration for every gRPC server + inter-broker client the broker opens.
             * {@link TlsConfig#DISABLED} keeps the plaintext default (existing tests depend on this).
             */
            TlsConfig tls,
            /**
             * Cluster default for the acks=all durability floor
             * ({@code min.insync.replicas}). Per-topic config overrides it;
             * topics with a smaller replication factor clamp down to RF.
             * Default {@link jbroker.broker.ProduceHandler#DEFAULT_MIN_INSYNC_REPLICAS}.
             */
            int minInsyncReplicas,
            /**
             * Retention-cleaner tick interval, ms. Default 300_000 (5 min).
             * Integration tests that need to observe retention within a few
             * seconds compress it.
             */
            long logCleanerIntervalMillis,
            /**
             * Disk-headroom watermark, bytes. While usable space on the data
             * volume sits below it, client produces are refused with
             * retriable {@code STORAGE_FULL}. Default 1 GiB.
             */
            long storageHeadroomBytes,
            /**
             * Cluster-wide defaults for per-topic storage/produce settings.
             * Per-topic config overrides each individually.
             */
            TopicDefaults topicDefaults,
            /**
             * Committed offsets of groups with no live members expire once
             * their newest commit is older than this, ms. Default 7 days;
             * non-positive disables expiry.
             */
            long offsetsRetentionMillis,
            /**
             * Client authentication mode. {@link AuthMode#MTLS} derives the
             * principal from the client certificate CN and rejects
             * principal-less RPCs; requires {@code tls} to be enabled.
             * Default {@link AuthMode#NONE} (startup logs a warning).
             */
            AuthMode authMode,
            /**
             * Principals that bypass ACL checks entirely. Inter-broker
             * certificate CNs belong here so replication and admin tooling
             * keep working the moment {@code auth.mode=mtls} turns on,
             * before any ACL exists. Empty by default.
             */
            java.util.Set<String> superUsers,
            /**
             * Bearer token the chaos HTTP endpoints demand on every
             * request. Blank (the default) leaves the chaos port open —
             * acceptable only for test harnesses; the server config layer
             * refuses to boot a chaos port without a token.
             */
            String chaosAuthToken,
            /**
             * Ceiling on the byte rate at which a partition reassignment's
             * "adding" replicas fetch during catch-up, per broker, so a
             * reassignment cannot starve live traffic. 0 (the default) is
             * unlimited.
             */
            long reassignmentThrottleBytesPerSec,
            /**
             * Per-principal produce byte-rate cap, per second. 0 (the
             * default) disables produce quotas entirely.
             */
            long produceQuotaBytesPerSec,
            /**
             * Per-principal byte-rate cap on client fetches, per second.
             * 0 (the default) disables fetch quotas entirely. Follower
             * replication is exempt by construction — it rides the
             * ReplicaFetch RPC, which has no quota gate — so this cap can
             * never starve the ISR.
             */
            long fetchQuotaBytesPerSec,
            /**
             * Redis URL backing the quota counters so per-principal caps
             * hold cluster-wide. Blank (the default) keeps counters
             * in-memory per broker — the same convention the admin event
             * fan-out uses for its Redis URL.
             */
            String quotaRedisUrl,
            /**
             * Rack / availability-zone label this broker declares for
             * itself (e.g. the K8s {@code topology.kubernetes.io/zone}
             * value). Peers learn it from heartbeats; topic placement and
             * drain planning spread replicas across the racks they see.
             * Blank (the default) = no rack.
             */
            String rack) {

        /**
         * Cluster defaults behind the per-topic keys: {@code
         * max.message.bytes}, {@code segment.bytes}, {@code retention.ms},
         * {@code retention.bytes}, {@code flush.messages}, {@code flush.ms}.
         * Negative retention/flush values mean unlimited / off.
         */
        public record TopicDefaults(
                int maxMessageBytes,
                long segmentBytes,
                long retentionMillis,
                long retentionBytes,
                long flushMessages,
                long flushMillis) {

            /** Kafka-shaped defaults: 1 MiB batches, 128 MiB segments, 7-day retention, flush off. */
            public static TopicDefaults standard() {
                return new TopicDefaults(
                        jbroker.broker.ProduceHandler.DEFAULT_MAX_MESSAGE_BYTES,
                        128L * 1024 * 1024,
                        TimeUnit.DAYS.toMillis(7),
                        -1L,
                        -1L,
                        -1L);
            }

            public TopicDefaults {
                if (maxMessageBytes < 1) {
                    throw new IllegalArgumentException("maxMessageBytes must be ≥ 1, got " + maxMessageBytes);
                }
                if (segmentBytes < 1) {
                    throw new IllegalArgumentException("segmentBytes must be ≥ 1, got " + segmentBytes);
                }
            }
        }

        public Config {
            voters = List.copyOf(voters);
            if (consumerOffsetsPartitions < 1) {
                throw new IllegalArgumentException(
                        "consumerOffsetsPartitions must be ≥ 1, got " + consumerOffsetsPartitions);
            }
            if (balancerTickMillis < 1L) {
                throw new IllegalArgumentException("balancerTickMillis must be ≥ 1, got " + balancerTickMillis);
            }
            if (balancerStabilityMillis < 0L) {
                throw new IllegalArgumentException(
                        "balancerStabilityMillis must be ≥ 0, got " + balancerStabilityMillis);
            }
            if (tls == null) tls = TlsConfig.DISABLED;
            if (minInsyncReplicas < 1) {
                throw new IllegalArgumentException("minInsyncReplicas must be ≥ 1, got " + minInsyncReplicas);
            }
            if (logCleanerIntervalMillis < 1L) {
                throw new IllegalArgumentException(
                        "logCleanerIntervalMillis must be ≥ 1, got " + logCleanerIntervalMillis);
            }
            if (storageHeadroomBytes < 1L) {
                throw new IllegalArgumentException("storageHeadroomBytes must be ≥ 1, got " + storageHeadroomBytes);
            }
            if (topicDefaults == null) topicDefaults = TopicDefaults.standard();
            if (authMode == null) authMode = AuthMode.NONE;
            if (authMode == AuthMode.MTLS && !tls.enabled()) {
                throw new IllegalArgumentException("auth.mode=mtls requires TLS on the broker listener");
            }
            superUsers = superUsers == null ? java.util.Set.of() : java.util.Set.copyOf(superUsers);
            if (chaosAuthToken == null) chaosAuthToken = "";
            if (reassignmentThrottleBytesPerSec < 0L) {
                throw new IllegalArgumentException(
                        "reassignmentThrottleBytesPerSec must be ≥ 0, got " + reassignmentThrottleBytesPerSec);
            }
            if (produceQuotaBytesPerSec < 0L) {
                throw new IllegalArgumentException(
                        "produceQuotaBytesPerSec must be ≥ 0, got " + produceQuotaBytesPerSec);
            }
            if (fetchQuotaBytesPerSec < 0L) {
                throw new IllegalArgumentException("fetchQuotaBytesPerSec must be ≥ 0, got " + fetchQuotaBytesPerSec);
            }
            if (quotaRedisUrl == null) quotaRedisUrl = "";
            if (rack == null) rack = "";
        }

        /** Pre-rack canonical shape kept callable: no rack declared. */
        public Config(
                NodeId selfId,
                Path dataDir,
                int raftPort,
                int brokerPort,
                List<VoterAddress> voters,
                int consumerOffsetsPartitions,
                int chaosPort,
                long balancerTickMillis,
                long balancerStabilityMillis,
                TlsConfig tls,
                int minInsyncReplicas,
                long logCleanerIntervalMillis,
                long storageHeadroomBytes,
                TopicDefaults topicDefaults,
                long offsetsRetentionMillis,
                AuthMode authMode,
                java.util.Set<String> superUsers,
                String chaosAuthToken,
                long reassignmentThrottleBytesPerSec,
                long produceQuotaBytesPerSec,
                long fetchQuotaBytesPerSec,
                String quotaRedisUrl) {
            this(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    authMode,
                    superUsers,
                    chaosAuthToken,
                    reassignmentThrottleBytesPerSec,
                    produceQuotaBytesPerSec,
                    fetchQuotaBytesPerSec,
                    quotaRedisUrl,
                    "");
        }

        /** Declare the rack / availability zone this broker runs in. Blank = none. */
        public Config withRack(String newRack) {
            return new Config(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    authMode,
                    superUsers,
                    chaosAuthToken,
                    reassignmentThrottleBytesPerSec,
                    produceQuotaBytesPerSec,
                    fetchQuotaBytesPerSec,
                    quotaRedisUrl,
                    newRack);
        }

        /** Pre-quota canonical shape kept callable: quotas disabled. */
        public Config(
                NodeId selfId,
                Path dataDir,
                int raftPort,
                int brokerPort,
                List<VoterAddress> voters,
                int consumerOffsetsPartitions,
                int chaosPort,
                long balancerTickMillis,
                long balancerStabilityMillis,
                TlsConfig tls,
                int minInsyncReplicas,
                long logCleanerIntervalMillis,
                long storageHeadroomBytes,
                TopicDefaults topicDefaults,
                long offsetsRetentionMillis,
                AuthMode authMode,
                java.util.Set<String> superUsers,
                String chaosAuthToken,
                long reassignmentThrottleBytesPerSec) {
            this(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    authMode,
                    superUsers,
                    chaosAuthToken,
                    reassignmentThrottleBytesPerSec,
                    0L,
                    0L,
                    "");
        }

        /** Pre-chaos-token canonical shape kept callable: open chaos port. */
        public Config(
                NodeId selfId,
                Path dataDir,
                int raftPort,
                int brokerPort,
                List<VoterAddress> voters,
                int consumerOffsetsPartitions,
                int chaosPort,
                long balancerTickMillis,
                long balancerStabilityMillis,
                TlsConfig tls,
                int minInsyncReplicas,
                long logCleanerIntervalMillis,
                long storageHeadroomBytes,
                TopicDefaults topicDefaults,
                long offsetsRetentionMillis,
                AuthMode authMode,
                java.util.Set<String> superUsers) {
            this(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    authMode,
                    superUsers,
                    "",
                    0L);
        }

        /** Set the bearer token the chaos HTTP endpoints demand. */
        public Config withChaosAuthToken(String token) {
            return new Config(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    authMode,
                    superUsers,
                    token,
                    reassignmentThrottleBytesPerSec,
                    produceQuotaBytesPerSec,
                    fetchQuotaBytesPerSec,
                    quotaRedisUrl,
                    rack);
        }

        /** Pre-super-users canonical shape kept callable: empty super-user set. */
        public Config(
                NodeId selfId,
                Path dataDir,
                int raftPort,
                int brokerPort,
                List<VoterAddress> voters,
                int consumerOffsetsPartitions,
                int chaosPort,
                long balancerTickMillis,
                long balancerStabilityMillis,
                TlsConfig tls,
                int minInsyncReplicas,
                long logCleanerIntervalMillis,
                long storageHeadroomBytes,
                TopicDefaults topicDefaults,
                long offsetsRetentionMillis,
                AuthMode authMode) {
            this(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    authMode,
                    java.util.Set.of());
        }

        /** Replace the super-user set (principals that bypass ACL checks). */
        public Config withSuperUsers(java.util.Set<String> users) {
            return new Config(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    authMode,
                    users,
                    chaosAuthToken,
                    reassignmentThrottleBytesPerSec,
                    produceQuotaBytesPerSec,
                    fetchQuotaBytesPerSec,
                    quotaRedisUrl,
                    rack);
        }

        /** Pre-auth-mode canonical shape kept callable: auth.mode=none. */
        public Config(
                NodeId selfId,
                Path dataDir,
                int raftPort,
                int brokerPort,
                List<VoterAddress> voters,
                int consumerOffsetsPartitions,
                int chaosPort,
                long balancerTickMillis,
                long balancerStabilityMillis,
                TlsConfig tls,
                int minInsyncReplicas,
                long logCleanerIntervalMillis,
                long storageHeadroomBytes,
                TopicDefaults topicDefaults,
                long offsetsRetentionMillis) {
            this(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    AuthMode.NONE);
        }

        /** Set the client authentication mode. Call after {@link #withTls} — mtls requires it. */
        public Config withAuthMode(AuthMode mode) {
            return new Config(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    mode,
                    superUsers,
                    chaosAuthToken,
                    reassignmentThrottleBytesPerSec,
                    produceQuotaBytesPerSec,
                    fetchQuotaBytesPerSec,
                    quotaRedisUrl,
                    rack);
        }

        /** Default idle-group offset retention: 7 days, matching Kafka. */
        public static final long DEFAULT_OFFSETS_RETENTION_MILLIS = 7L * 24 * 60 * 60 * 1000;

        /** Pre-offsets-expiry canonical shape kept callable: 7-day retention. */
        public Config(
                NodeId selfId,
                Path dataDir,
                int raftPort,
                int brokerPort,
                List<VoterAddress> voters,
                int consumerOffsetsPartitions,
                int chaosPort,
                long balancerTickMillis,
                long balancerStabilityMillis,
                TlsConfig tls,
                int minInsyncReplicas,
                long logCleanerIntervalMillis,
                long storageHeadroomBytes,
                TopicDefaults topicDefaults) {
            this(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    DEFAULT_OFFSETS_RETENTION_MILLIS,
                    AuthMode.NONE);
        }

        /** Pre-topic-defaults canonical shape kept callable: standard defaults. */
        public Config(
                NodeId selfId,
                Path dataDir,
                int raftPort,
                int brokerPort,
                List<VoterAddress> voters,
                int consumerOffsetsPartitions,
                int chaosPort,
                long balancerTickMillis,
                long balancerStabilityMillis,
                TlsConfig tls,
                int minInsyncReplicas,
                long logCleanerIntervalMillis,
                long storageHeadroomBytes) {
            this(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    TopicDefaults.standard());
        }

        /** Replace the cluster-wide per-topic defaults. */
        public Config withTopicDefaults(TopicDefaults defaults) {
            return new Config(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    defaults,
                    offsetsRetentionMillis,
                    authMode,
                    superUsers,
                    chaosAuthToken,
                    reassignmentThrottleBytesPerSec,
                    produceQuotaBytesPerSec,
                    fetchQuotaBytesPerSec,
                    quotaRedisUrl,
                    rack);
        }

        /** Override the idle-group offset retention window. Non-positive disables expiry. */
        public Config withOffsetsRetentionMillis(long retentionMillis) {
            return new Config(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    retentionMillis,
                    authMode,
                    superUsers,
                    chaosAuthToken,
                    reassignmentThrottleBytesPerSec,
                    produceQuotaBytesPerSec,
                    fetchQuotaBytesPerSec,
                    quotaRedisUrl,
                    rack);
        }

        /** Pre-headroom canonical shape kept callable: 1 GiB watermark. */
        public Config(
                NodeId selfId,
                Path dataDir,
                int raftPort,
                int brokerPort,
                List<VoterAddress> voters,
                int consumerOffsetsPartitions,
                int chaosPort,
                long balancerTickMillis,
                long balancerStabilityMillis,
                TlsConfig tls,
                int minInsyncReplicas,
                long logCleanerIntervalMillis) {
            this(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    jbroker.broker.DiskHeadroom.DEFAULT_HEADROOM_BYTES);
        }

        /** Pre-cleaner-knob canonical shape kept callable: 5-minute cleaner tick. */
        public Config(
                NodeId selfId,
                Path dataDir,
                int raftPort,
                int brokerPort,
                List<VoterAddress> voters,
                int consumerOffsetsPartitions,
                int chaosPort,
                long balancerTickMillis,
                long balancerStabilityMillis,
                TlsConfig tls,
                int minInsyncReplicas) {
            this(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    300_000L);
        }

        /** Pre-min-ISR canonical shape kept callable: floor defaults to 2. */
        public Config(
                NodeId selfId,
                Path dataDir,
                int raftPort,
                int brokerPort,
                List<VoterAddress> voters,
                int consumerOffsetsPartitions,
                int chaosPort,
                long balancerTickMillis,
                long balancerStabilityMillis,
                TlsConfig tls) {
            this(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    jbroker.broker.ProduceHandler.DEFAULT_MIN_INSYNC_REPLICAS);
        }

        /** Pre-existing-call-sites overload: balancer timings default to 15_000 / 30_000. */
        public Config(
                NodeId selfId,
                Path dataDir,
                int raftPort,
                int brokerPort,
                List<VoterAddress> voters,
                int consumerOffsetsPartitions,
                int chaosPort) {
            this(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    15_000L,
                    30_000L,
                    TlsConfig.DISABLED);
        }

        /** Full-9-arg overload keeping the pre-TLS shape callable. */
        public Config(
                NodeId selfId,
                Path dataDir,
                int raftPort,
                int brokerPort,
                List<VoterAddress> voters,
                int consumerOffsetsPartitions,
                int chaosPort,
                long balancerTickMillis,
                long balancerStabilityMillis) {
            this(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    TlsConfig.DISABLED);
        }

        /** 5-arg ctor back-compat: chaos disabled (chaosPort = -1). */
        public Config(
                NodeId selfId,
                Path dataDir,
                int raftPort,
                int brokerPort,
                List<VoterAddress> voters,
                int consumerOffsetsPartitions) {
            this(selfId, dataDir, raftPort, brokerPort, voters, consumerOffsetsPartitions, -1);
        }

        public Config withChaosPort(int port) {
            return new Config(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    port,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    authMode,
                    superUsers,
                    chaosAuthToken,
                    reassignmentThrottleBytesPerSec,
                    produceQuotaBytesPerSec,
                    fetchQuotaBytesPerSec,
                    quotaRedisUrl,
                    rack);
        }

        /** Set or replace the TLS bundle on an existing Config. */
        public Config withTls(TlsConfig newTls) {
            return new Config(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    newTls == null ? TlsConfig.DISABLED : newTls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    authMode,
                    superUsers,
                    chaosAuthToken,
                    reassignmentThrottleBytesPerSec,
                    produceQuotaBytesPerSec,
                    fetchQuotaBytesPerSec,
                    quotaRedisUrl,
                    rack);
        }

        /**
         * Override the cluster-default acks=all floor. Integration tests
         * that exercise NOT_ENOUGH_REPLICAS raise it; the soak scenario
         * runs the shipped default.
         */
        public Config withMinInsyncReplicas(int n) {
            return new Config(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    n,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    authMode,
                    superUsers,
                    chaosAuthToken,
                    reassignmentThrottleBytesPerSec,
                    produceQuotaBytesPerSec,
                    fetchQuotaBytesPerSec,
                    quotaRedisUrl,
                    rack);
        }

        /**
         * Convenience overload for tests with explicit voters. Defaults
         * {@code consumerOffsetsPartitions} to 1 so existing ITs that don't
         * exercise consumer-group routing don't pay a 50-partition tax in
         * BrokerFencer / replication cycles. Production multi-broker
         * deployments should use the explicit-N constructor (or
         * {@link #withConsumerOffsetsPartitions}) to opt up to the
         * Kafka-convention 50.
         */
        public Config(NodeId selfId, Path dataDir, int raftPort, int brokerPort, List<VoterAddress> voters) {
            this(selfId, dataDir, raftPort, brokerPort, voters, /*consumerOffsetsPartitions*/ 1);
        }

        /**
         * Back-compat overload: single-voter config where the only voter is self,
         * reachable at 127.0.0.1 on the provided {@code raftPort}. All existing
         * single-broker callers keep working unchanged.
         *
         * <p>Defaults to a 1-partition {@code __consumer_offsets} to keep
         * single-broker test fixtures fast — production deployments should
         * use the explicit-voter constructor (which defaults to 50).
         */
        public Config(NodeId selfId, Path dataDir, int raftPort, int brokerPort) {
            this(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    List.of(new VoterAddress(selfId, "127.0.0.1", raftPort, brokerPort)),
                    /*consumerOffsetsPartitions*/ 1);
        }

        public Config withConsumerOffsetsPartitions(int n) {
            return new Config(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    n,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    authMode,
                    superUsers,
                    chaosAuthToken,
                    reassignmentThrottleBytesPerSec,
                    produceQuotaBytesPerSec,
                    fetchQuotaBytesPerSec,
                    quotaRedisUrl,
                    rack);
        }

        /**
         * Override the preferred-leader balancer's tick interval and
         * stability window. Integration tests that need to observe a rebalance
         * within a few seconds compress both. Production callers should leave
         * this alone (defaults 15s / 30s).
         */
        public Config withBalancerTiming(long tickMillis, long stabilityMillis) {
            return new Config(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    tickMillis,
                    stabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    authMode,
                    superUsers,
                    chaosAuthToken,
                    reassignmentThrottleBytesPerSec,
                    produceQuotaBytesPerSec,
                    fetchQuotaBytesPerSec,
                    quotaRedisUrl,
                    rack);
        }

        /**
         * Compress the retention-cleaner tick. Integration tests that need
         * to observe segment deletion within a few seconds use this;
         * production callers should leave the 5-minute default alone.
         */
        public Config withLogCleanerIntervalMillis(long intervalMillis) {
            return new Config(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    intervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    authMode,
                    superUsers,
                    chaosAuthToken,
                    reassignmentThrottleBytesPerSec,
                    produceQuotaBytesPerSec,
                    fetchQuotaBytesPerSec,
                    quotaRedisUrl,
                    rack);
        }

        /**
         * Override the disk-headroom watermark. Integration tests set it
         * above the volume's actual free space to observe the degraded
         * mode; production callers size it to their write rate.
         */
        public Config withStorageHeadroomBytes(long headroomBytes) {
            return new Config(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    headroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    authMode,
                    superUsers,
                    chaosAuthToken,
                    reassignmentThrottleBytesPerSec,
                    produceQuotaBytesPerSec,
                    fetchQuotaBytesPerSec,
                    quotaRedisUrl,
                    rack);
        }

        /**
         * Cap the byte rate at which reassignment newcomers catch up, per
         * broker. 0 (the default) is unlimited. Set it low to keep a
         * reassignment from starving live produce/consume traffic.
         */
        public Config withReassignmentThrottleBytesPerSec(long bytesPerSec) {
            return new Config(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    authMode,
                    superUsers,
                    chaosAuthToken,
                    bytesPerSec,
                    produceQuotaBytesPerSec,
                    fetchQuotaBytesPerSec,
                    quotaRedisUrl,
                    rack);
        }

        /**
         * Set per-principal byte-rate quotas for the produce and client
         * fetch paths. 0 disables that path's quota; both at 0 (the
         * default) keeps quota enforcement off entirely.
         */
        public Config withQuotaBytesPerSec(long produceBytesPerSec, long fetchBytesPerSec) {
            return new Config(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    authMode,
                    superUsers,
                    chaosAuthToken,
                    reassignmentThrottleBytesPerSec,
                    produceBytesPerSec,
                    fetchBytesPerSec,
                    quotaRedisUrl,
                    rack);
        }

        /**
         * Back the quota counters with Redis so per-principal caps hold
         * across brokers. Blank (the default) keeps counters in-memory,
         * per broker.
         */
        public Config withQuotaRedisUrl(String url) {
            return new Config(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    voters,
                    consumerOffsetsPartitions,
                    chaosPort,
                    balancerTickMillis,
                    balancerStabilityMillis,
                    tls,
                    minInsyncReplicas,
                    logCleanerIntervalMillis,
                    storageHeadroomBytes,
                    topicDefaults,
                    offsetsRetentionMillis,
                    authMode,
                    superUsers,
                    chaosAuthToken,
                    reassignmentThrottleBytesPerSec,
                    produceQuotaBytesPerSec,
                    fetchQuotaBytesPerSec,
                    url,
                    rack);
        }
    }

    private final TopicManager topicManager;
    private final LogManager logManager;
    private final WaitingStateMachine waitingSm;
    private final RaftDriver raftDriver;
    private final Server brokerServer;
    private final int brokerPort;
    private final ScheduledExecutorService isrTicker;
    private final ScheduledExecutorService registrationTicker;
    private final ScheduledExecutorService fencerTicker;
    private final ScheduledExecutorService balancerTicker;
    private final jbroker.broker.BrokerRegistry brokerRegistry;
    private final jbroker.broker.BrokerLiveness brokerLiveness;
    private final jbroker.broker.BrokerHeartbeatSender heartbeatSender;
    private final jbroker.broker.replication.ReplicaFetcherManager fetcherManager;
    private final jbroker.broker.replication.DefaultFetcherFactory fetcherFactory;
    private final jbroker.broker.BrokerMetrics metrics;
    private final jbroker.broker.chaos.ChaosHttpServer chaosHttp;
    private final jbroker.broker.DiskHeadroom diskHeadroom;
    private final jbroker.broker.quota.QuotaEnforcer quotaEnforcer;
    private final jbroker.broker.controller.MembershipController membershipController;
    private final jbroker.broker.controller.DecommissionController decommissionController;
    private final jbroker.broker.controller.PreferredLeaderBalancer preferredBalancer;
    private final jbroker.broker.txn.TxnCoordinatorRuntime txnRuntime;
    private final jbroker.broker.txn.TxnMarkerPeerClients txnMarkerClients;
    private final int selfBrokerId;

    private Broker(
            TopicManager tm,
            LogManager lm,
            WaitingStateMachine wsm,
            RaftDriver raftDriver,
            Server brokerServer,
            int brokerPort,
            ScheduledExecutorService isrTicker,
            ScheduledExecutorService registrationTicker,
            ScheduledExecutorService fencerTicker,
            ScheduledExecutorService balancerTicker,
            jbroker.broker.BrokerRegistry brokerRegistry,
            jbroker.broker.BrokerLiveness brokerLiveness,
            jbroker.broker.BrokerHeartbeatSender heartbeatSender,
            jbroker.broker.replication.ReplicaFetcherManager fetcherManager,
            jbroker.broker.replication.DefaultFetcherFactory fetcherFactory,
            jbroker.broker.BrokerMetrics metrics,
            jbroker.broker.chaos.ChaosHttpServer chaosHttp,
            jbroker.broker.DiskHeadroom diskHeadroom,
            jbroker.broker.quota.QuotaEnforcer quotaEnforcer,
            jbroker.broker.controller.MembershipController membershipController,
            jbroker.broker.controller.DecommissionController decommissionController,
            jbroker.broker.controller.PreferredLeaderBalancer preferredBalancer,
            jbroker.broker.txn.TxnCoordinatorRuntime txnRuntime,
            jbroker.broker.txn.TxnMarkerPeerClients txnMarkerClients,
            int selfBrokerId) {
        this.topicManager = tm;
        this.logManager = lm;
        this.waitingSm = wsm;
        this.raftDriver = raftDriver;
        this.brokerServer = brokerServer;
        this.brokerPort = brokerPort;
        this.isrTicker = isrTicker;
        this.registrationTicker = registrationTicker;
        this.fencerTicker = fencerTicker;
        this.balancerTicker = balancerTicker;
        this.brokerRegistry = brokerRegistry;
        this.brokerLiveness = brokerLiveness;
        this.heartbeatSender = heartbeatSender;
        this.fetcherManager = fetcherManager;
        this.fetcherFactory = fetcherFactory;
        this.metrics = metrics;
        this.chaosHttp = chaosHttp;
        this.diskHeadroom = diskHeadroom;
        this.quotaEnforcer = quotaEnforcer;
        this.membershipController = membershipController;
        this.decommissionController = decommissionController;
        this.preferredBalancer = preferredBalancer;
        this.txnRuntime = txnRuntime;
        this.txnMarkerClients = txnMarkerClients;
        this.selfBrokerId = selfBrokerId;
    }

    public static Broker start(Config config) throws IOException {
        Files.createDirectories(config.dataDir());
        var raftDir = config.dataDir().resolve("raft");
        var topicsDir = config.dataDir().resolve("topics");
        Files.createDirectories(raftDir);
        Files.createDirectories(topicsDir);

        // --- Raft layer (metadata log) ---
        var raftLog = FileRaftLog.open(raftDir.resolve("log.bin"));
        var state = FilePersistentState.open(raftDir.resolve("state.bin"));
        var voterIds = config.voters().stream().map(VoterAddress::id).toList();
        var raftConfig = new RaftConfig(
                config.selfId(),
                voterIds,
                TimeUnit.MILLISECONDS.toNanos(500),
                TimeUnit.MILLISECONDS.toNanos(250),
                TimeUnit.MILLISECONDS.toNanos(100),
                100);
        var core = new DefaultRaftCore(raftConfig, raftLog, state, Long.MAX_VALUE);

        var topicManager = new TopicManager();
        var producerIdRegistry = new ProducerIdRegistry();

        // --- LogManager (partition data logs) ---
        // Cluster defaults come from Config.topicDefaults (standard():
        // Kafka-shaped 7-day retention, no size limit, 128 MiB segments,
        // flush off). Per-topic retention.ms / retention.bytes /
        // segment.bytes / flush.* overrides resolve through the topic
        // catalogue below.
        var defaults = config.topicDefaults();
        var logManager = new LogManager(
                topicsDir,
                new LogManager.Config(
                        defaults.segmentBytes(),
                        defaults.retentionMillis(),
                        defaults.retentionBytes(),
                        jbroker.storage.LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        config.logCleanerIntervalMillis()));
        logManager.setTopicLogConfigResolver(topic -> topicManager
                .describe(topic)
                .map(d -> d.compact()
                        // Compacted topics keep every key's latest value forever —
                        // the retention passes must never delete their segments.
                        // (__consumer_offsets is compacted; time-deleting it would
                        // silently reset idle groups' committed positions.)
                        ? new LogManager.TopicLogConfig(
                                d.effectiveSegmentBytes(defaults.segmentBytes()),
                                -1L,
                                -1L,
                                d.effectiveFlushMessages(defaults.flushMessages()),
                                d.effectiveFlushMillis(defaults.flushMillis()))
                        : new LogManager.TopicLogConfig(
                                d.effectiveSegmentBytes(defaults.segmentBytes()),
                                d.effectiveRetentionMillis(defaults.retentionMillis()),
                                d.effectiveRetentionBytes(defaults.retentionBytes()),
                                d.effectiveFlushMessages(defaults.flushMessages()),
                                d.effectiveFlushMillis(defaults.flushMillis()))));

        // Per-broker event publisher for the admin-facing SSE stream.
        // Declared here so the leader-epoch listener (next block) can
        // capture it. Wired into the MetadataServiceHandler at the bottom
        // of start() where the stream RPC reads from it.
        var eventPublisher = new jbroker.broker.events.BrokerEventPublisher();

        // Whenever a partition's leader_epoch bumps and self is the
        // new leader, record (epoch, current_leo) in the partition's
        // LeaderEpochCheckpoint so OffsetsForLeaderEpoch can answer.
        int selfId = config.selfId().value();
        // Track the most recent leader-change wall-clock so the
        // PreferredLeaderBalancer can skip proposals during periods of
        // churn (stability window).
        var lastLeaderChangeMillis = new java.util.concurrent.atomic.AtomicLong(System.currentTimeMillis());
        MetadataStateMachine.LeaderEpochListener leaderEpochListener = (topic, partition, epoch, leaderId) -> {
            lastLeaderChangeMillis.set(System.currentTimeMillis());
            if (leaderId == selfId) {
                try {
                    var leo = logManager.logFor(topic, partition).nextOffset();
                    logManager.leaderEpochCheckpoint(topic, partition).assign(epoch, leo);
                } catch (IOException e) {
                    log.warn("failed to record leader-epoch checkpoint for {}-{}", topic, partition, e);
                }
            }
            // Emit a leader_changed event for admin SSE, regardless
            // of whether self is the new leader. Old-leader id is unavailable
            // at this call site (listener doesn't carry prior state) so we
            // encode -1; admin UIs use the epoch to detect consecutive bumps.
            try {
                var publisher = eventPublisher;
                if (publisher != null) {
                    long id = publisher.allocateId();
                    publisher.publish(new jbroker.broker.events.BrokerEvent.LeaderChanged(
                            id, topic, partition, -1, leaderId, epoch));
                }
            } catch (Exception ignored) {
                // Event publishing is observability-only; never let a bug in
                // the publisher block metadata apply.
            }
        };
        var brokerRegistry = new jbroker.broker.BrokerRegistry();
        // Self-declared rack. Peers learn it from this broker's heartbeats;
        // the local note makes the controller's own rack visible to
        // placement without waiting for a heartbeat round-trip.
        brokerRegistry.noteRack(config.selfId().value(), config.rack());
        // Audit-finding #1 — one ProducerStateManager per broker, shared
        // between this broker's ProduceHandler (leader dedup) and every
        // ReplicaFetcher (follower observe). Keeps idempotent-producer
        // dedup state correct across leader failover.
        var producerState = new jbroker.broker.ProducerStateManager();
        // Transactional producer-epoch floors, shared by the produce path
        // (fencing), the marker append path (markers carry the fencing
        // bump), and the replica apply path (an ex-follower taking over
        // leadership fences with the same floor). Created before the
        // fetcher factory so no early fetcher misses the feed.
        var txnEpochs = new jbroker.broker.txn.TxnPartitionEpochs();
        // Audit-finding #3 — single ChaosState instance shared by the inbound
        // ChaosServerInterceptor (constructed later with the gRPC server) and
        // by outbound ChaosClientInterceptors threaded through RaftPeerClient,
        // ReplicaPeerClient (via DefaultFetcherFactory), and
        // BrokerHeartbeatSender. Hoisted here so all outbound factories can
        // reference it.
        var chaosState = new jbroker.broker.chaos.ChaosState();
        int selfBrokerId = config.selfId().value();
        java.util.function.IntFunction<io.grpc.ClientInterceptor> chaosInterceptorFactory =
                peerId -> new jbroker.broker.chaos.ChaosClientInterceptor(chaosState, selfBrokerId, peerId);
        var fetcherFactory = new jbroker.broker.replication.DefaultFetcherFactory(
                config.selfId().value(),
                logManager,
                topicManager,
                /*pollIntervalMs*/ 25L,
                /*fetchTimeoutMs*/ 2_000L,
                config.tls(),
                producerState,
                chaosInterceptorFactory);
        fetcherFactory.setTxnPartitionEpochs(txnEpochs);
        var fetcherManager = new jbroker.broker.replication.ReplicaFetcherManager(
                config.selfId().value(), topicManager, brokerRegistry, logManager, fetcherFactory);
        // Deferred: the Raft driver is built below (a construction cycle runs
        // metadataSm -> regChain -> raftDriver -> waitingSm -> metadataSm), but
        // the listener only fires once records commit, long after start.
        var raftDriverRef = new java.util.concurrent.atomic.AtomicReference<RaftDriver>();
        MetadataStateMachine.BrokerRegistrationListener regChain = (bid, h, pt, ah, ap, rp) -> {
            brokerRegistry.onBrokerRegistration(bid, h, pt, ah, ap, rp);
            fetcherManager.onBrokerRegistration(bid, h, pt, ah, ap, rp);
            // Form a Raft dial channel to a dynamically-joined broker straight
            // from its replicated registration, so peers appear cluster-wide
            // without a static-config edit. Skip self, legacy records with no
            // raft port, and channels already held (the listener re-fires on
            // every re-proposed registration).
            var peerId = new NodeId(bid);
            var driver = raftDriverRef.get();
            if (driver != null && !peerId.equals(config.selfId()) && rp > 0 && !driver.hasPeer(peerId)) {
                driver.addPeer(
                        peerId, new RaftPeerClient(peerId, h, rp, config.tls(), chaosInterceptorFactory.apply(bid)));
            }
            long id = eventPublisher.allocateId();
            eventPublisher.publish(new jbroker.broker.events.BrokerEvent.BrokerRegistered(id, bid, h, pt));
        };
        // On DeleteTopic, evict LogManager cache + segment files so
        // topic-recreation with the same name can't pick up stale offsets,
        // and kick the replica-fetcher reconcile loop so any live fetcher
        // stops as soon as the metadata record applies.
        MetadataStateMachine.TopicDeletionListener topicDeletionChain = deletedTopic -> {
            logManager.deleteTopicDir(deletedTopic);
            fetcherManager.scheduleReconcile();
            // Audit-finding #1 — drop this topic's dedup entries so a
            // topic recreated with the same name starts with a clean slate.
            producerState.evictTopic(deletedTopic);
            // Same clean-slate rule for the transactional epoch floors.
            txnEpochs.evictTopic(deletedTopic);
        };
        var metadataSm = new MetadataStateMachine(
                topicManager, producerIdRegistry, leaderEpochListener, regChain, fetcherManager, topicDeletionChain);
        var waitingSm = new WaitingStateMachine(metadataSm);

        // Meter reassignment catch-up: a fetcher whose partition is being
        // reassigned and where self is an "adding" replica (in the target set
        // but not the original) pulls at most the throttle's byte budget, so it
        // cannot starve live traffic. Every other fetch gets the full ceiling.
        int selfBroker = config.selfId().value();
        var reassignmentThrottle = new jbroker.broker.replication.ReassignmentThrottle(
                config.reassignmentThrottleBytesPerSec(), System::nanoTime);
        fetcherFactory.setFetchThrottle((topic, partition) -> {
            var pending = metadataSm.reassignments().get(topic, partition).orElse(null);
            if (pending == null
                    || !pending.target().contains(selfBroker)
                    || pending.original().contains(selfBroker)) {
                return jbroker.broker.replication.ReplicaFetcher.DEFAULT_MAX_FETCH_BYTES;
            }
            return reassignmentThrottle.reserve(jbroker.broker.replication.ReplicaFetcher.DEFAULT_MAX_FETCH_BYTES);
        });

        // --- Raft transport (peer map built from static voter set) ---
        var peerMap = new java.util.HashMap<NodeId, RaftPeerClient>();
        for (var v : config.voters()) {
            if (v.id().equals(config.selfId())) continue;
            peerMap.put(
                    v.id(),
                    new RaftPeerClient(
                            v.id(),
                            v.host(),
                            v.raftPort(),
                            config.tls(),
                            chaosInterceptorFactory.apply(v.id().value())));
        }
        var raftDriver = new RaftDriver(
                config.selfId(), core, waitingSm, Map.copyOf(peerMap), TimeUnit.MILLISECONDS.toNanos(30));
        raftDriverRef.set(raftDriver);
        raftDriver.start(config.raftPort(), config.tls());

        // Broker-join orchestration: on the controller (Raft leader), drive a
        // newcomer from non-voting learner to voter once its log catches up.
        // Ticked from the registration loop below; requestAddBroker kicks it off.
        var membershipController =
                new jbroker.broker.controller.MembershipController(new RaftMembershipAdapter(raftDriver));

        // --- Broker gRPC server ---
        var brokerMetrics = new jbroker.broker.BrokerMetrics();
        var fetchSessionCache = new jbroker.broker.FetchSessionCache();
        // Quota admission for client produce and fetch. Both rates at
        // zero — the default — selects the no-op enforcer, so an
        // unconfigured broker behaves exactly as before. Replication is
        // never charged: followers pull via the ReplicaFetch RPC, which
        // bypasses both gated handlers.
        var quotaEnforcer = jbroker.broker.quota.QuotaEnforcer.configured(
                config.produceQuotaBytesPerSec(), config.fetchQuotaBytesPerSec(), config.quotaRedisUrl());
        var fetch = new FetchHandler(logManager, topicManager, fetchSessionCache, brokerMetrics, quotaEnforcer);
        var followerTracker = new FollowerStateTracker();
        // Probes the volume backing the partition logs; ProduceHandler
        // refuses client produces while usable space is below the watermark.
        var diskHeadroom = new jbroker.broker.DiskHeadroom(topicsDir, config.storageHeadroomBytes());
        var produce = new ProduceHandler(
                logManager,
                topicManager,
                config.selfId().value(),
                followerTracker,
                brokerMetrics,
                quotaEnforcer,
                producerState,
                config.minInsyncReplicas(),
                diskHeadroom,
                defaults.maxMessageBytes());
        var replicaFetch = new ReplicaFetchHandler(
                logManager, topicManager, config.selfId().value(), followerTracker, System::currentTimeMillis);
        var offsetsForLeaderEpoch = new OffsetsForLeaderEpochHandler(
                logManager, topicManager, config.selfId().value());
        AdminHandler.MetadataProposer proposer = (payload, timeoutMs) -> {
            var fut = waitingSm.awaitApply(payload);
            raftDriver.propose(payload);
            fut.get(timeoutMs, TimeUnit.MILLISECONDS);
        };
        // AdminHandler learns Raft leader id + registry so NOT_LEADER
        // responses can carry suggested_leader_* hints that the admin REST
        // layer surfaces into the error envelope.
        // Wire LogManager::compactLogNowIfPresent so the
        // ForceCompactPartition admin RPC can trigger a synchronous compaction
        // on the responding broker's local log without touching Raft.
        //
        // The leader lookup must be role-aware: RaftCore.currentLeader()
        // is populated only on followers (a leader never receives its own
        // AppendEntries), and AdminHandler's not-leader fast path gates
        // proposals on this lookup — the naive currentLeader() would make
        // the actual leader reject its own mutations.
        final int selfIdForAdmin = config.selfId().value();
        var admin = new AdminHandler(
                topicManager,
                proposer,
                selfIdForAdmin,
                brokerRegistry::knownBrokerIds,
                () -> raftDriver.role() == jbroker.raft.core.Role.LEADER
                        ? java.util.Optional.of(selfIdForAdmin)
                        : raftDriver.currentLeader().map(jbroker.raft.core.NodeId::value),
                brokerRegistry,
                logManager::compactLogNowIfPresent);
        admin.setAclStore(metadataSm.aclStore());
        var initProducerId = new InitProducerIdHandler(producerIdRegistry, proposer);

        var brokerLiveness = new jbroker.broker.BrokerLiveness();
        var heartbeatHandler =
                new jbroker.broker.BrokerHeartbeatHandler(brokerLiveness, System::nanoTime, brokerRegistry::noteRack);

        // Decommission orchestration: drain every replica off a leaving
        // broker via reassignments, then drop it from the Raft voter set.
        // Ticked with the membership controller below; decommissionBroker
        // kicks it off. "Live" mirrors the fencer's judgment: a fresh
        // heartbeat within the stale threshold, and self is always live.
        final long decommissionLiveThresholdNanos = TimeUnit.SECONDS.toNanos(3);
        var decommissionController = new jbroker.broker.controller.DecommissionController(
                new jbroker.broker.controller.DecommissionController.ClusterAccess() {
                    @Override
                    public boolean isLeader() {
                        return raftDriver.role() == jbroker.raft.core.Role.LEADER;
                    }

                    @Override
                    public java.util.List<jbroker.broker.PartitionAssignment> assignments() {
                        return topicManager.allPartitionAssignments();
                    }

                    @Override
                    public java.util.Set<Integer> liveBrokers() {
                        var out = new java.util.HashSet<Integer>();
                        long now = System.nanoTime();
                        for (int id : brokerRegistry.knownBrokerIds()) {
                            if (id == selfBrokerId) {
                                out.add(id);
                                continue;
                            }
                            var sig = brokerLiveness.lastSignal(id);
                            if (sig.isPresent() && now - sig.get().wallClockNanos() <= decommissionLiveThresholdNanos) {
                                out.add(id);
                            }
                        }
                        return out;
                    }

                    @Override
                    public java.util.Map<Integer, String> racks() {
                        return brokerRegistry.racks();
                    }

                    @Override
                    public boolean reassignmentPending(String topic, int partition) {
                        return metadataSm.reassignments().get(topic, partition).isPresent();
                    }

                    @Override
                    public boolean startReassignment(String topic, int partition, java.util.List<Integer> target)
                            throws InterruptedException {
                        return proposeReassignment(raftDriver, waitingSm, topicManager, topic, partition, target);
                    }

                    @Override
                    public java.util.List<NodeId> voters() {
                        return raftDriver.activeVoters();
                    }

                    @Override
                    public void proposeMembership(jbroker.raft.core.Membership membership) throws InterruptedException {
                        raftDriver.proposeMembership(membership);
                    }
                });

        // Group coordinator: in-memory state for groups whose coordinator
        // partition this broker leads. The heartbeat path is wired; the
        // per-coord-partition activation listener that rebuilds state on
        // coordinator failover is still to come.
        var groupCoordinator = new GroupCoordinator(
                topic -> topicManager.describe(topic).map(td -> td.partitions()).orElse(0), new RangeAssignor());
        // In-memory offset commit/fetch cache. The recovery walk
        // (below, after gRPC server start) re-reads any __consumer_offsets
        // partitions this broker leads after Raft replay finishes;
        // coordinator-failover-time recovery is still to come.
        var offsetCache = new OffsetCache();
        // Persist GroupMetadataValue records on every membership
        // change. Encodes via ConsumerOffsetsTopic.valueForGroupMetadata
        // and self-produces a Type-2 record into the appropriate
        // __consumer_offsets partition (key namespaced via 0x02). The
        // recovery walk re-reads these on broker startup or coordinator
        // activation; latest-record-per-key wins.
        groupCoordinator.setSnapshotListener((groupId, snap) -> {
            try {
                appendGroupMetadataSnapshot(topicManager, logManager, groupId, snap);
            } catch (Exception e) {
                log.debug("group metadata snapshot append failed for {}; will retry on next change", groupId, e);
            }
        });
        var consumerHandler = new ConsumerHandler(
                topicManager,
                logManager,
                brokerRegistry,
                groupCoordinator,
                offsetCache,
                config.selfId().value(),
                System::nanoTime,
                System::currentTimeMillis);
        // One authorizer for every enforcement point. Under
        // auth.mode=none it is a constant-true gate.
        var authorizer =
                new jbroker.broker.auth.Authorizer(config.authMode(), metadataSm.aclStore(), config.superUsers());
        produce.setAuthorizer(authorizer);
        fetch.setAuthorizer(authorizer);
        consumerHandler.setAuthorizer(authorizer);
        admin.setAuthorizer(authorizer);

        // --- Transactions ---
        // Produce-side fencing shares the marker/replica epoch floors.
        produce.setTxnPartitionEpochs(txnEpochs);
        // Leader-local marker appends, served to remote coordinators via
        // the TxnMarkers service and used directly for local partitions.
        final int selfIdForTxn = config.selfId().value();
        var txnMarkerWriter = new jbroker.broker.txn.TxnMarkerWriter(
                logManager, topicManager, followerTracker, selfIdForTxn, config.minInsyncReplicas(), txnEpochs);
        // Markers landing on __consumer_offsets decide staged transactional
        // offset commits: fold into the committed view + durable re-append
        // of the folded records (see ConsumerHandler.onTxnMarker).
        txnMarkerWriter.setMarkerListener((topic, partition, pid, epoch, commit) -> {
            if (jbroker.broker.ConsumerOffsetsTopic.NAME.equals(topic)) {
                consumerHandler.onTxnMarker(partition, pid, epoch, commit);
            }
        });
        var txnMarkersHandler = new jbroker.broker.txn.TxnMarkersHandler(txnMarkerWriter);
        // Marker fan-out to peers rides the internal addresses, like
        // replication; chaos partitions apply to it like any inter-broker
        // RPC.
        var txnMarkerClients = new jbroker.broker.txn.TxnMarkerPeerClients(
                config.tls(), chaosInterceptorFactory, brokerRegistry::addressFor);
        jbroker.broker.txn.TxnCoordinatorRuntime.MarkerTransport markerTransport = instruction -> {
            var tp = instruction.tp();
            var ps = topicManager.partitionState(tp.getTopic(), tp.getPartition());
            if (ps.isEmpty() || ps.get().leader() <= 0) return false;
            int leader = ps.get().leader();
            if (leader == selfIdForTxn) {
                return txnMarkerWriter.append(
                                tp.getTopic(),
                                tp.getPartition(),
                                instruction.producerId(),
                                instruction.producerEpoch(),
                                instruction.commit(),
                                instruction.coordinatorEpoch())
                        == jbroker.broker.ErrorCodes.NONE;
            }
            var markerReq = jbroker.proto.txn.WriteTxnMarkersRequest.newBuilder()
                    .addMarkers(jbroker.proto.txn.TxnMarker.newBuilder()
                            .setProducerId(instruction.producerId())
                            .setProducerEpoch(instruction.producerEpoch())
                            .setCommit(instruction.commit())
                            .setCoordinatorEpoch(instruction.coordinatorEpoch())
                            .addPartitions(tp))
                    .build();
            var resp = txnMarkerClients.write(leader, markerReq);
            return resp.isPresent()
                    && resp.get().getResultsCount() == 1
                    && resp.get().getResults(0).getError() == jbroker.proto.common.ErrorCode.OK;
        };
        // Fresh producer ids come from the controller's replicated counter,
        // the same record shape InitProducerId commits.
        jbroker.broker.txn.TxnCoordinatorRuntime.ProducerIdAllocator txnPidAllocator = () -> {
            long id = producerIdRegistry.allocateNext();
            var record = jbroker.proto.raft.MetadataRecord.newBuilder()
                    .setProducerIdAssignment(jbroker.proto.raft.ProducerIdAssignmentRecord.newBuilder()
                            .setProducerId(id)
                            .setProducerEpoch(0)
                            .setNextProducerId(id + 1)
                            .build())
                    .build();
            proposer.proposeAndWait(record.toByteArray(), TimeUnit.SECONDS.toMillis(5));
            return id;
        };
        var txnRuntime = new jbroker.broker.txn.TxnCoordinatorRuntime(
                logManager,
                topicManager,
                followerTracker,
                selfIdForTxn,
                config.minInsyncReplicas(),
                txnPidAllocator,
                markerTransport,
                System::currentTimeMillis);
        var txnHandler = new jbroker.broker.txn.TxnHandler(topicManager, brokerRegistry, selfIdForTxn, txnRuntime);
        txnHandler.setAuthorizer(authorizer);
        // Metadata service: DescribeCluster now wired to live
        // BrokerRegistry + BrokerLiveness + Raft state. Remaining RPCs
        // return UNIMPLEMENTED until they are implemented.
        //
        // RaftCore.currentLeader() tracks the leader only on followers —
        // the incumbent leader never populates its own leaderId field (it
        // doesn't receive AppendEntries from itself). So when self is
        // LEADER, fall back to self; otherwise ask Raft.
        final int selfBrokerIdForMeta = config.selfId().value();
        java.util.function.Supplier<java.util.Optional<Integer>> controllerIdSupplier = () -> {
            if (raftDriver.role() == jbroker.raft.core.Role.LEADER) {
                return java.util.Optional.of(selfBrokerIdForMeta);
            }
            return raftDriver.currentLeader().map(jbroker.raft.core.NodeId::value);
        };
        var partitionMetricsProvider = new jbroker.broker.metrics.DefaultPartitionMetricsProvider(
                selfBrokerIdForMeta, topicManager, logManager, followerTracker);
        var metadataHandler = new MetadataServiceHandler(
                selfBrokerIdForMeta,
                brokerRegistry,
                brokerLiveness,
                () -> raftDriver.role().toString(),
                controllerIdSupplier,
                () -> raftDriver.currentTerm().value(),
                // metadata_offset is a forward-compat field.
                // WaitingStateMachine doesn't yet expose applied-offset; stub
                // to 0L for now.
                () -> 0L,
                System::nanoTime,
                MetadataServiceHandler.DEFAULT_STALENESS_NANOS,
                topicManager,
                logManager,
                groupCoordinator,
                offsetCache,
                raftDriver::observability,
                brokerMetrics,
                eventPublisher,
                partitionMetricsProvider);
        metadataHandler.setDiskHeadroom(diskHeadroom);
        // Cooperative chaos control plane. Only active when the
        // config exposes a chaos port (≥ 0); interceptor is installed on
        // every inbound service so pause + peer-block affect broker-to-
        // broker RPCs as well as client traffic.
        // Audit-finding #3 — {@code chaosState} was hoisted above so every
        // outbound factory can reuse it; here we only need the inbound
        // interceptor.
        var chaosInterceptor = new jbroker.broker.chaos.ChaosServerInterceptor(chaosState);
        if (config.authMode() == AuthMode.NONE) {
            log.warn("auth.mode=none — every client is anonymous and nothing is rejected; "
                    + "set auth.mode=mtls before exposing this broker to a real network");
        }
        // Auth is added after chaos so it intercepts first: unauthenticated
        // calls are rejected before any chaos gate or handler runs.
        var brokerServerBuilder = NettyServerBuilder.forPort(config.brokerPort())
                .intercept(chaosInterceptor)
                .intercept(new MtlsAuthInterceptor(config.authMode()))
                .addService(BrokerGrpcServices.producer(produce, initProducerId))
                .addService(BrokerGrpcServices.consumer(fetch, consumerHandler))
                .addService(BrokerGrpcServices.replicaConsumer(replicaFetch, offsetsForLeaderEpoch))
                .addService(BrokerGrpcServices.cluster(heartbeatHandler))
                .addService(BrokerGrpcServices.admin(admin, consumerHandler))
                .addService(BrokerGrpcServices.metadata(metadataHandler))
                .addService(BrokerGrpcServices.txn(txnHandler))
                .addService(BrokerGrpcServices.txnOffsets(consumerHandler))
                .addService(BrokerGrpcServices.txnMarkers(txnMarkersHandler));
        try {
            var sslCtx = TlsContexts.serverContext(config.tls());
            if (sslCtx != null) brokerServerBuilder.sslContext(sslCtx);
        } catch (javax.net.ssl.SSLException e) {
            throw new IOException("TLS server context build failed", e);
        }
        var server = brokerServerBuilder.build().start();

        jbroker.broker.chaos.ChaosHttpServer chaosHttp = null;
        if (config.chaosPort() >= 0) {
            chaosHttp = new jbroker.broker.chaos.ChaosHttpServer(
                    chaosState,
                    config.selfId().value(),
                    config.chaosPort(),
                    eventPublisher,
                    // Force-election now wired. RaftDriver queues a
                    // self-addressed TimeoutNow so the core jumps straight to
                    // startElection at term+1 (skipping pre-vote). No-op on
                    // LEADER.
                    () -> {
                        try {
                            raftDriver.forceElection();
                            log.info(
                                    "chaos: force-election triggered on broker {}",
                                    config.selfId().value());
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    },
                    () -> System.exit(137),
                    config.chaosAuthToken());
            log.info(
                    "chaos HTTP server listening on port {}{}",
                    chaosHttp.port(),
                    config.chaosAuthToken().isBlank() ? " (no auth token — test mode)" : "");
        }

        // Wait for Raft to complete a first election. In a multi-voter
        // cluster this broker may or may not be the winner; instead of
        // blocking on "self is LEADER", wait for the term to advance past
        // 0, which is a cluster-wide signal that an election ran.
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline && raftDriver.currentTerm().value() == 0) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Whoever is the Raft leader is responsible for proposing
        // BrokerRegistrationRecords for every voter that isn't already in
        // the registry. This avoids the "follower proposes get silently
        // dropped" trap: non-leader proposes can't commit, so self-
        // registration from a non-leader would never land.
        //
        // The leader uses the static voter config to synthesise each
        // broker's address; follower brokers can't contribute data here,
        // but the address they bound at freePort()-time must match the
        // port they advertised back to us in their voter entry — tests
        // construct voters with bound ports to guarantee this.
        final int selfIdVal = config.selfId().value();
        // Partitions whose offset log has already been replayed into
        // the OffsetCache. Set guards against re-walking on every tick.
        var recoveredPartitions = new java.util.concurrent.ConcurrentHashMap<Integer, Boolean>();
        var registrationTicker = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "broker-registration-" + selfIdVal);
            t.setDaemon(true);
            return t;
        });
        // __consumer_offsets needs to exist before any consumer-group
        // RPC can land. The leader proposes it once after registrations
        // settle; the creator is idempotent so re-ticking under election
        // churn doesn't matter.
        var consumerOffsetsCreator = new ConsumerOffsetsCreator(
                topicManager,
                brokerRegistry::knownBrokerIds,
                config.selfId().value(),
                ConsumerOffsetsCreator.fromMetadataProposer(proposer),
                () -> raftDriver.role() == jbroker.raft.core.Role.LEADER,
                config.consumerOffsetsPartitions());
        // __transaction_state rides the same creator machinery AND the
        // same partition-count knob: both coordinator topics share the
        // routing-stability caveat (the count is fixed at first boot) and
        // the same scale — production configs default to the canonical 50
        // ({@link jbroker.broker.txn.TxnStateTopic#PARTITION_COUNT}),
        // test fixtures shrink both to 1 so 3-broker ITs don't pay a
        // 100-replica-fetcher tax.
        var txnStateCreator = new ConsumerOffsetsCreator(
                topicManager,
                brokerRegistry::knownBrokerIds,
                config.selfId().value(),
                ConsumerOffsetsCreator.fromMetadataProposer(proposer),
                () -> raftDriver.role() == jbroker.raft.core.Role.LEADER,
                config.consumerOffsetsPartitions(),
                jbroker.broker.txn.TxnStateTopic.NAME);
        // Offset expiry (R2.7): each coordinator drops idle groups' commits
        // once their newest commit outlives the retention window. Runs
        // every minute; the pass itself is a no-op unless a group
        // qualifies, and a non-positive retention disables it entirely.
        registrationTicker.scheduleWithFixedDelay(
                () -> {
                    try {
                        consumerHandler.expireStaleOffsets(config.offsetsRetentionMillis());
                    } catch (RuntimeException e) {
                        log.warn("offset-expiry tick failed; will retry", e);
                    }
                },
                60_000L,
                60_000L,
                TimeUnit.MILLISECONDS);
        registrationTicker.scheduleWithFixedDelay(
                () -> {
                    // Raft-leader-only work: propose BrokerRegistration
                    // records and __consumer_offsets CreateTopic. Both are
                    // Raft proposals and only the Raft leader can make them.
                    if (raftDriver.role() == jbroker.raft.core.Role.LEADER) {
                        for (var v : config.voters()) {
                            int bid = v.id().value();
                            // Benign race: the registry may be updated between
                            // this check and the propose, producing one extra
                            // duplicate record per election. applyBrokerRegistration
                            // is idempotent (last-writer-wins overwrite to the
                            // same value) so the duplicate is harmless.
                            if (brokerRegistry.addressFor(bid).isPresent()) continue;
                            try {
                                // Propagate the advertised listener
                                // fields into the registration record so
                                // client-facing RPCs (FindCoordinator,
                                // DescribeCluster) on every broker can
                                // return an externally-reachable address.
                                var brokerRecord = jbroker.proto.raft.BrokerRegistrationRecord.newBuilder()
                                        .setBrokerId(bid)
                                        .setHost(v.host())
                                        .setPort(v.brokerPort())
                                        // Publish the Raft peer port so every broker can dial a
                                        // dynamically-joined broker straight from its registration.
                                        .setRaftPort(v.raftPort());
                                if (v.hasAdvertised()) {
                                    brokerRecord
                                            .setAdvertisedHost(v.advertisedHost())
                                            .setAdvertisedPort(v.advertisedBrokerPort());
                                }
                                var record = jbroker.proto.raft.MetadataRecord.newBuilder()
                                        .setBroker(brokerRecord.build())
                                        .build()
                                        .toByteArray();
                                raftDriver.propose(record);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            } catch (Exception e) {
                                log.debug("registration propose for broker {} failed; retrying", bid, e);
                            }
                        }
                        // After every registration tick, give the
                        // __consumer_offsets creator a chance to land. Idempotent:
                        // it self-skips when the topic already exists or when
                        // no peers are known yet.
                        try {
                            consumerOffsetsCreator.ensureCreated();
                        } catch (Exception e) {
                            log.debug("__consumer_offsets create attempt failed; will retry next tick", e);
                        }
                        try {
                            txnStateCreator.ensureCreated();
                        } catch (Exception e) {
                            log.debug("__transaction_state create attempt failed; will retry next tick", e);
                        }
                        // Advance any in-flight broker join (add-learner ->
                        // catch-up -> promote). Idempotent per phase; a no-op
                        // when no join is pending.
                        try {
                            membershipController.runOnce();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (Exception e) {
                            log.debug("membership tick failed; will retry", e);
                        }
                        // Advance any in-flight decommission (drain ->
                        // voter removal). Same idempotent-per-tick contract.
                        try {
                            decommissionController.runOnce();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (Exception e) {
                            log.debug("decommission tick failed; will retry", e);
                        }
                    }
                    // Recovery is NOT gated on Raft leadership: every broker
                    // must bootstrap its OffsetCache + GroupCoordinator for
                    // __consumer_offsets partitions IT leads locally. After
                    // a coordinator failover, the new partition
                    // leader may well be a Raft follower — the recovery walk
                    // still has to run so a fetchOffsets RPC arriving at the
                    // new coordinator returns the committed offset instead
                    // of OFFSET_OUT_OF_RANGE.
                    try {
                        recoverNewlyOwnedOffsetPartitions(
                                topicManager,
                                logManager,
                                offsetCache,
                                groupCoordinator,
                                recoveredPartitions,
                                selfIdVal);
                    } catch (Exception e) {
                        log.warn("offset cache recovery failed on broker {}; will retry next tick", selfIdVal, e);
                    }
                    // Same unconditional rule for transaction coordination:
                    // every broker activates coordinators for the
                    // __transaction_state partitions IT leads (replaying the
                    // log and resuming a dead predecessor's pending marker
                    // deliveries), and sweeps transaction timeouts.
                    try {
                        txnRuntime.tick();
                    } catch (Exception e) {
                        log.warn("txn coordinator tick failed on broker {}; will retry next tick", selfIdVal, e);
                    }
                },
                0,
                1,
                TimeUnit.SECONDS);

        // ISR housekeeping ticker: every 2s, ask IsrManager for any
        // (leader, ISR) flips and propose them through Raft. 10s lag
        // timeout matches Kafka's default replica.lag.time.max.ms.
        var isr = new IsrManager(
                config.selfId().value(), topicManager, logManager, followerTracker, TimeUnit.SECONDS.toMillis(10));
        // Advances in-flight partition reassignments on the controller: expand
        // the replica set, wait for the ISR machinery to grow the newcomers,
        // then contract to the target. Ticked alongside the ISR housekeeper.
        var reassignmentDriver = new jbroker.broker.ReassignmentDriver(topicManager, metadataSm.reassignments());
        var isrTicker = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "isr-manager");
            t.setDaemon(true);
            return t;
        });
        isrTicker.scheduleWithFixedDelay(
                () -> {
                    try {
                        for (var proposal : isr.decideChanges(System.currentTimeMillis())) {
                            var fut = waitingSm.awaitApply(proposal);
                            raftDriver.propose(proposal);
                            fut.get(3, TimeUnit.SECONDS);
                        }
                        // Reassignment is a controller operation: only the Raft
                        // leader drives it (its proposals name a partition's
                        // replicas/leader, not just the ISR it owns).
                        if (raftDriver.role() == jbroker.raft.core.Role.LEADER) {
                            for (var proposal : reassignmentDriver.decide()) {
                                var fut = waitingSm.awaitApply(proposal);
                                raftDriver.propose(proposal);
                                fut.get(3, TimeUnit.SECONDS);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        // Election windows + shutdown races are expected; log
                        // at debug so real bugs (NPE/CCE/logic errors) are
                        // still surfaced on any appender set above debug.
                        log.debug("ISR tick failed, retrying on next tick", e);
                    }
                },
                2,
                2,
                TimeUnit.SECONDS);

        // BrokerFencer: 1s tick; on the active controller only, scan
        // BrokerLiveness for stale partition leaders and propose a
        // PartitionChangeRecord demoting each one. A fenced broker's
        // partition-leaderships flip to the first surviving ISR member
        // with a bumped leader_epoch; no surviving ISR → leader = -1
        // sentinel.
        jbroker.broker.replication.BrokerFencer.MetadataProposer fenceProposer = payload -> {
            try {
                raftDriver.propose(payload);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        var fencer = new jbroker.broker.replication.BrokerFencer(
                config.selfId().value(),
                topicManager,
                brokerLiveness,
                fenceProposer,
                raftDriver::role,
                TimeUnit.SECONDS.toNanos(3));
        var fencerTicker = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "broker-fencer-" + config.selfId().value());
            t.setDaemon(true);
            return t;
        });
        fencerTicker.scheduleWithFixedDelay(
                () -> {
                    try {
                        fencer.tick(System.nanoTime());
                    } catch (Exception e) {
                        log.debug("fencer tick failed, retrying on next tick", e);
                    }
                    try {
                        // Drop members past their session timeout.
                        // Runs on every broker (each one is the coordinator
                        // for its own __consumer_offsets partitions) so the
                        // tick is unconditional, not leader-gated.
                        groupCoordinator.tickEvictions(System.nanoTime());
                    } catch (Exception e) {
                        log.debug("group eviction tick failed, retrying on next tick", e);
                    }
                },
                /*initialDelay*/ 250,
                // P10-flake-fix: 250ms tick (was 1s). Failover detection
                // latency budget = last-heartbeat-age (≤250ms) +
                // staleness-threshold (3s) + next-tick (≤250ms) = ≤3.5s.
                // MultiBrokerFailoverIT's 5s budget then has 1.5s of
                // headroom for PartitionChangeRecord propose + commit +
                // apply, which is comfortable even on slow CI fsync.
                // The 1s cadence left only 0.75s headroom, which CI
                // runners routinely blew through.
                /*period*/ 250,
                TimeUnit.MILLISECONDS);

        // PreferredLeaderBalancer ticker. On the active controller
        // only, every {@code balancerTickMillis}: if no leader change happened
        // in the last {@code balancerStabilityMillis}, for every partition
        // whose preferred replica (replicas[0]) is in ISR but isn't the
        // current leader, propose a PartitionChangeRecord moving leadership
        // back. Stops partition leadership from drifting onto a single
        // broker after a wave of failovers.
        //
        // Timings pulled from config so integration tests can
        // compress the schedule. Defaults remain 15s tick / 30s stability.
        var preferredBalancer = new jbroker.broker.controller.PreferredLeaderBalancer(
                topicManager,
                () -> raftDriver.role() == jbroker.raft.core.Role.LEADER,
                lastLeaderChangeMillis::get,
                config.balancerStabilityMillis());
        // Cluster-lifecycle admin RPCs delegate to the controllers built
        // above; wired late (like the ACL store) because the handler is
        // constructed before the controllers exist.
        admin.setClusterOperations(new AdminHandler.ClusterOperations() {
            @Override
            public boolean addBroker(int brokerId, String host, int raftPort, int brokerPort)
                    throws InterruptedException {
                return requestAddBroker(
                        raftDriver, waitingSm, membershipController, new NodeId(brokerId), host, raftPort, brokerPort);
            }

            @Override
            public boolean decommissionBroker(int brokerId) {
                if (brokerId == selfBrokerId) return false;
                return decommissionController.requestDecommission(brokerId);
            }

            @Override
            public boolean reassignPartition(String topic, int partition, java.util.List<Integer> replicas)
                    throws InterruptedException {
                return proposeReassignment(raftDriver, waitingSm, topicManager, topic, partition, replicas);
            }

            @Override
            public int rebalanceLeadership() throws InterruptedException {
                return proposePreferredMoves(topicManager, raftDriver, preferredBalancer.proposeRebalancesNow());
            }

            @Override
            public jbroker.broker.controller.MembershipController.Progress joinProgress() {
                return membershipController.progress();
            }

            @Override
            public jbroker.broker.controller.DecommissionController.Progress decommissionProgress() {
                return decommissionController.progress();
            }

            @Override
            public java.util.List<NodeId> voters() {
                return raftDriver.activeVoters();
            }

            @Override
            public java.util.List<jbroker.broker.ReassignmentStore.Pending> reassignments() {
                return metadataSm.reassignments().list();
            }
        });

        var balancerTicker = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "preferred-leader-balancer-" + config.selfId().value());
            t.setDaemon(true);
            return t;
        });
        balancerTicker.scheduleWithFixedDelay(
                () -> {
                    try {
                        proposePreferredMoves(
                                topicManager,
                                raftDriver,
                                preferredBalancer.proposeRebalances(System.currentTimeMillis()));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        log.debug("preferred-leader balancer tick failed, retrying", e);
                    }
                },
                /*initialDelay*/ config.balancerTickMillis(),
                /*period*/ config.balancerTickMillis(),
                TimeUnit.MILLISECONDS);

        // Point-to-point heartbeat sender: every broker periodically
        // pings every peer (excluding self) with its current metadata
        // offset. Receivers update their BrokerLiveness maps directly.
        var heartbeatPeers = new java.util.ArrayList<jbroker.broker.BrokerHeartbeatSender.PeerAddress>();
        for (var v : config.voters()) {
            if (v.id().equals(config.selfId())) continue;
            heartbeatPeers.add(
                    new jbroker.broker.BrokerHeartbeatSender.PeerAddress(v.id().value(), v.host(), v.brokerPort()));
        }
        // metadata-offset is a forward-compat field in the heartbeat RPC;
        // the fencer only looks at wall-clock, so a placeholder 0 is
        // fine here. Wire up a real applied-offset supplier if a consumer
        // needs it.
        // 250ms heartbeat with 3s staleness threshold = 12 missed
        // heartbeats to trigger a false-positive fence — safe under
        // heavy GC / JVM load. Low enough traffic (6 RPCs/s per broker
        // in a 3-broker cluster) to be negligible.
        // Pause-aware: the sender consults ChaosState.isPaused so
        // `pause-broker` from the chaos UI flips the broker to "dead" on
        // every peer's BrokerLiveness, the fencer kicks in, and the
        // admin's health pill correctly flips to yellow/red.
        var heartbeatSender = new jbroker.broker.BrokerHeartbeatSender(
                config.selfId().value(),
                heartbeatPeers,
                () -> 0L,
                /*intervalMs*/ 250L,
                chaosState::isPaused,
                config.tls(),
                chaosInterceptorFactory,
                config.rack());
        heartbeatSender.start();

        return new Broker(
                topicManager,
                logManager,
                waitingSm,
                raftDriver,
                server,
                server.getPort(),
                isrTicker,
                registrationTicker,
                fencerTicker,
                balancerTicker,
                brokerRegistry,
                brokerLiveness,
                heartbeatSender,
                fetcherManager,
                fetcherFactory,
                brokerMetrics,
                chaosHttp,
                diskHeadroom,
                quotaEnforcer,
                membershipController,
                decommissionController,
                preferredBalancer,
                txnRuntime,
                txnMarkerClients,
                selfId);
    }

    /** Exposed for tests + the admin-app chaos proxy probe. */
    public int chaosPort() {
        return chaosHttp == null ? -1 : chaosHttp.port();
    }

    /** Broker-local counter bag (incremental-fetch hits, etc). */
    public jbroker.broker.BrokerMetrics metrics() {
        return metrics;
    }

    public jbroker.broker.BrokerRegistry brokerRegistry() {
        return brokerRegistry;
    }

    public jbroker.broker.BrokerLiveness brokerLiveness() {
        return brokerLiveness;
    }

    public int brokerPort() {
        return brokerPort;
    }

    public TopicManager topics() {
        return topicManager;
    }

    public jbroker.raft.core.Role role() {
        return raftDriver.role();
    }

    /** The current Raft voter set, for membership observability. */
    public List<NodeId> raftVoters() {
        return raftDriver.activeVoters();
    }

    /**
     * Ask this broker, acting as the controller, to admit a broker into the
     * cluster: publish its address so every broker forms a Raft dial channel to
     * it, then propose it as a non-voting learner, wait for its log to catch up,
     * and promote it to voter. The address is committed synchronously (so this
     * leader has a channel to the newcomer before naming it in a membership
     * change); the learner-join then advances on the registration tick.
     *
     * <p>Returns {@code false} if this broker is not the Raft leader, the
     * registration did not commit, a join is already in flight, or {@code id} is
     * already a voter. Poll {@link #membershipProgress()} to watch a started
     * join. The newcomer must already be running, booted as a learner (its
     * config's voters are the incumbents, itself absent).
     *
     * @param id         node id of the joining broker
     * @param host       its inter-broker dial host
     * @param raftPort   its Raft peer port
     * @param brokerPort its client-facing gRPC port
     */
    public boolean requestAddBroker(NodeId id, String host, int raftPort, int brokerPort) throws InterruptedException {
        return requestAddBroker(raftDriver, waitingSm, membershipController, id, host, raftPort, brokerPort);
    }

    /** Shared join path: the public entry point above and the admin RPC adapter both land here. */
    private static boolean requestAddBroker(
            RaftDriver raftDriver,
            WaitingStateMachine waitingSm,
            jbroker.broker.controller.MembershipController membershipController,
            NodeId id,
            String host,
            int raftPort,
            int brokerPort)
            throws InterruptedException {
        if (raftDriver.role() != jbroker.raft.core.Role.LEADER) return false;
        var record = jbroker.proto.raft.MetadataRecord.newBuilder()
                .setBroker(jbroker.proto.raft.BrokerRegistrationRecord.newBuilder()
                        .setBrokerId(id.value())
                        .setHost(host)
                        .setPort(brokerPort)
                        .setRaftPort(raftPort)
                        .build())
                .build()
                .toByteArray();
        try {
            var applied = waitingSm.awaitApply(record);
            raftDriver.propose(record);
            applied.get(5, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            // Registration did not commit (lost leadership, quorum stall);
            // the caller retries against the current leader.
            return false;
        }
        return membershipController.requestAddBroker(id);
    }

    /** Snapshot of the in-flight (or most recent) broker join, for observability. */
    public jbroker.broker.controller.MembershipController.Progress membershipProgress() {
        return membershipController.progress();
    }

    /**
     * Ask this controller to decommission broker {@code brokerId}: drain every
     * replica it hosts onto other live brokers (refused up front if any
     * partition would lose replication factor), then remove it from the Raft
     * voter set. The drain advances on the controller tick; poll
     * {@link #decommissionProgress()} to watch it. Self-decommission is
     * refused — removing the active controller's own vote would abandon the
     * drain mid-flight when leadership moves; ask another broker instead.
     */
    public boolean decommissionBroker(int brokerId) {
        if (brokerId == selfBrokerId) return false;
        return decommissionController.requestDecommission(brokerId);
    }

    /** Snapshot of the in-flight (or most recent) decommission, for observability. */
    public jbroker.broker.controller.DecommissionController.Progress decommissionProgress() {
        return decommissionController.progress();
    }

    /**
     * Operator-requested preferred-leader rebalance: move every partition
     * whose preferred replica (replicas[0]) is in sync but not leading back
     * onto it, immediately — the periodic balancer's stability window is
     * deliberately skipped, the operator has decided now is the moment.
     * Returns the number of moves proposed (0 on a balanced cluster or when
     * this broker is not the controller).
     */
    public int rebalanceLeadership() throws InterruptedException {
        return proposePreferredMoves(topicManager, raftDriver, preferredBalancer.proposeRebalancesNow());
    }

    /**
     * Ask this controller to reassign a partition's replicas to {@code target}.
     * Publishes a reassignment record (target + the current set as the original,
     * so a cancel can revert) and blocks until it commits; the controller's tick
     * then drives the change — expand, wait for catch-up, contract. Returns
     * {@code false} if this broker is not the Raft leader, the partition is
     * unknown, or the record did not commit. Cancel by reassigning back to the
     * original set before the leader switches.
     */
    public boolean reassignPartition(String topic, int partition, java.util.List<Integer> target)
            throws InterruptedException {
        return proposeReassignment(raftDriver, waitingSm, topicManager, topic, partition, target);
    }

    /**
     * Propose the leadership moves the preferred-leader balancer decided on
     * — shared by the periodic balancer tick and the on-demand
     * {@link #rebalanceLeadership()}. Each record carries the epochs it was
     * derived from, so a proposal computed from a stale view is dropped at
     * apply. Returns the number of moves proposed.
     */
    private static int proposePreferredMoves(
            TopicManager topicManager,
            RaftDriver raftDriver,
            List<jbroker.broker.controller.PreferredLeaderBalancer.Proposal> proposals)
            throws InterruptedException {
        int proposed = 0;
        for (var proposal : proposals) {
            var ps = topicManager
                    .partitionState(proposal.topic(), proposal.partition())
                    .orElse(null);
            if (ps == null) continue;
            var record = jbroker.proto.raft.PartitionChangeRecord.newBuilder()
                    .setTopic(proposal.topic())
                    .setPartition(proposal.partition())
                    .setLeader(proposal.newLeader())
                    .addAllIsr(ps.isr())
                    .addAllReplicas(ps.replicas())
                    .setLeaderEpoch(proposal.leaderEpoch())
                    .setPartitionEpoch(0)
                    // CAS guard: dropped at apply if the partition state
                    // moved since ps was read (stale balancer view).
                    .setPriorLeaderEpoch(ps.leaderEpoch())
                    .setPriorPartitionEpoch(ps.partitionEpoch())
                    .build();
            var payload = jbroker.proto.raft.MetadataRecord.newBuilder()
                    .setPartitionChange(record)
                    .build()
                    .toByteArray();
            raftDriver.propose(payload);
            proposed++;
            log.info(
                    "preferred-leader: {}-{} → broker {} (epoch {})",
                    proposal.topic(),
                    proposal.partition(),
                    proposal.newLeader(),
                    proposal.leaderEpoch());
        }
        return proposed;
    }

    /**
     * Shared propose path for reassignments: used by the public
     * {@link #reassignPartition} entry point and by the decommission
     * controller's drain (which starts one per affected partition).
     */
    private static boolean proposeReassignment(
            RaftDriver raftDriver,
            WaitingStateMachine waitingSm,
            TopicManager topicManager,
            String topic,
            int partition,
            java.util.List<Integer> target)
            throws InterruptedException {
        if (raftDriver.role() != jbroker.raft.core.Role.LEADER) return false;
        var state = topicManager.partitionState(topic, partition).orElse(null);
        if (state == null) return false;
        var record = jbroker.proto.raft.MetadataRecord.newBuilder()
                .setPartitionReassignment(jbroker.proto.raft.PartitionReassignmentRecord.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .addAllTargetReplicas(target)
                        .addAllOriginalReplicas(state.replicas())
                        .build())
                .build()
                .toByteArray();
        try {
            var applied = waitingSm.awaitApply(record);
            raftDriver.propose(record);
            applied.get(5, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            return false;
        }
        return true;
    }

    /** Test-only accessor for assertion-driven inspection of per-broker logs. */
    public LogManager logManager() {
        return logManager;
    }

    /**
     * Test-only hook for staging a {@code PartitionChangeRecord}
     * through the local Raft driver. Integration tests that need to
     * produce a specific cluster topology (e.g. leader ≠ replicas[0] so
     * the PreferredLeaderBalancer has something to rebalance) call this
     * on the current Raft leader. Blocks until the local state machine
     * has applied the record.
     *
     * <p>Package-private on purpose — production callers should go
     * through {@code AdminHandler} or one of the controller tickers.
     */
    void proposePartitionChangeForTest(
            String topic,
            int partition,
            int leader,
            java.util.List<Integer> isr,
            java.util.List<Integer> replicas,
            int leaderEpoch,
            int partitionEpoch,
            long timeoutMillis)
            throws Exception {
        var pc = jbroker.proto.raft.PartitionChangeRecord.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setLeader(leader)
                .addAllIsr(isr)
                .addAllReplicas(replicas)
                .setLeaderEpoch(leaderEpoch)
                .setPartitionEpoch(partitionEpoch)
                .build();
        byte[] payload = jbroker.proto.raft.MetadataRecord.newBuilder()
                .setPartitionChange(pc)
                .build()
                .toByteArray();
        var fut = waitingSm.awaitApply(payload);
        raftDriver.propose(payload);
        fut.get(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * For each {@code __consumer_offsets} partition this
     * broker leads, walk the log twice (Type-1 records into the
     * {@link OffsetCache}, Type-2 records into the
     * {@link GroupCoordinator}). Subsequent ticks skip recovered
     * partitions. Both walks are idempotent and can run in either order.
     */
    private static void recoverNewlyOwnedOffsetPartitions(
            jbroker.broker.TopicManager topicManager,
            LogManager logManager,
            OffsetCache offsetCache,
            GroupCoordinator groupCoordinator,
            java.util.Map<Integer, Boolean> recovered,
            int selfBrokerId)
            throws IOException {
        var topicDesc = topicManager.describe(jbroker.broker.ConsumerOffsetsTopic.NAME);
        if (topicDesc.isEmpty()) return;
        int partitionCount = topicDesc.get().partitions();
        for (int p = 0; p < partitionCount; p++) {
            if (recovered.containsKey(p)) continue;
            var ps = topicManager.partitionState(jbroker.broker.ConsumerOffsetsTopic.NAME, p);
            if (ps.isEmpty() || ps.get().leader() != selfBrokerId) continue;
            // Staging-aware walk: transactional offset batches replay into
            // the coordinator's live staging (so a marker arriving after
            // activation still decides them), markers fold/discard in log
            // order, plain records apply directly.
            int offsetsApplied = jbroker.broker.group.OffsetCacheRecovery.rebuild(
                    logManager,
                    jbroker.broker.ConsumerOffsetsTopic.NAME,
                    p,
                    offsetCache,
                    groupCoordinator.txnOffsetStaging());
            jbroker.broker.group.GroupMetadataRecovery.rebuild(logManager, p, groupCoordinator, System.nanoTime());
            recovered.put(p, Boolean.TRUE);
            log.info(
                    "recovered __consumer_offsets-{} on broker {}: {} offset-commit records applied",
                    p,
                    selfBrokerId,
                    offsetsApplied);
        }
    }

    /**
     * Encode and append a single Type-2 record carrying the
     * group's latest snapshot into its coordinator partition. The
     * partition is determined by {@code floorMod(group.hashCode(),
     * partitionCount)}, matching the {@code FindCoordinator} routing.
     */
    private static void appendGroupMetadataSnapshot(
            jbroker.broker.TopicManager topicManager,
            LogManager logManager,
            String groupId,
            jbroker.broker.ConsumerOffsetsTopic.GroupMetadataValue snapshot)
            throws IOException {
        var topicDesc =
                topicManager.describe(jbroker.broker.ConsumerOffsetsTopic.NAME).orElseThrow();
        int partition = Math.floorMod(groupId.hashCode(), topicDesc.partitions());
        byte[] key = jbroker.broker.ConsumerOffsetsTopic.keyForGroupMetadata(groupId);
        byte[] value = jbroker.broker.ConsumerOffsetsTopic.valueForGroupMetadata(snapshot);
        var record = new jbroker.storage.Record(0, 0L, key, value);
        var records = java.util.List.of(record);
        var log = logManager.logFor(jbroker.broker.ConsumerOffsetsTopic.NAME, partition);
        var buf = java.nio.ByteBuffer.allocate(jbroker.storage.RecordBatch.estimatedSize(records));
        long base = log.nextOffset();
        long now = System.currentTimeMillis();
        // Stamp the current leader epoch: the partition's epoch lineage
        // must stay non-decreasing (transaction markers stamp the true
        // epoch, and a 0-stamped batch after a post-failover marker would
        // livelock follower reconciliation).
        int leaderEpoch = topicManager
                .partitionState(jbroker.broker.ConsumerOffsetsTopic.NAME, partition)
                .map(ps -> ps.leaderEpoch())
                .orElse(0);
        jbroker.storage.RecordBatch.encode(
                buf,
                base,
                leaderEpoch,
                now,
                now, /*producerId*/
                -1L, /*producerEpoch*/
                (short) -1, /*baseSequence*/
                -1,
                records);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        log.appendRaw(bytes, base);
    }

    /**
     * Simulates {@code kill -9} in-process: shuts gRPC + Raft down with
     * {@code shutdownNow()} and skips the graceful drain. Tests use this
     * to prove failover from an abruptly-gone partition leader.
     */
    /**
     * Controlled shutdown (R7.2): stop the preferred-leader balancer so it
     * cannot fight the handoff, drain every led partition to another ISR
     * member, then run the normal {@link #close()}. The drain is bounded
     * by {@code drainTimeoutMillis}; whatever could not hand off by then
     * (sole-ISR partitions, proposals lost to races at the deadline)
     * closes anyway — those partitions go offline and recover onto this
     * broker per ISR-only election when it returns. SIGTERM routes here
     * through the BrokerApp shutdown hook; plain {@code close()} stays
     * the fast path for tests.
     */
    public void closeGracefully(long drainTimeoutMillis) {
        balancerTicker.shutdownNow();
        try {
            drainLeadership(drainTimeoutMillis);
        } catch (RuntimeException e) {
            log.warn("leadership drain failed; closing anyway", e);
        }
        close();
    }

    /**
     * Propose-and-await handoff rounds until no partition led by this
     * broker has another ISR member to take over, or the deadline passes.
     * Each round recomputes proposals from fresh state and stamps CAS
     * priors, so a proposal dropped at apply (state moved underneath it)
     * simply gets re-derived next round. Proposals go through
     * {@code raftDriver.propose}, which forwards to the Raft leader when
     * this broker is a follower.
     */
    private void drainLeadership(long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        int rounds = 0;
        while (System.currentTimeMillis() < deadline) {
            var proposals = jbroker.broker.controller.LeadershipDrainer.proposeDrain(topicManager, selfBrokerId);
            if (proposals.isEmpty()) {
                log.info("leadership drain complete after {} round(s)", rounds);
                return;
            }
            rounds++;
            for (var proposal : proposals) {
                var ps = topicManager
                        .partitionState(proposal.topic(), proposal.partition())
                        .orElse(null);
                if (ps == null || ps.leader() != selfBrokerId) continue;
                var record = jbroker.proto.raft.PartitionChangeRecord.newBuilder()
                        .setTopic(proposal.topic())
                        .setPartition(proposal.partition())
                        .setLeader(proposal.newLeader())
                        .addAllIsr(ps.isr())
                        .addAllReplicas(ps.replicas())
                        .setLeaderEpoch(proposal.newLeaderEpoch())
                        .setPartitionEpoch(0)
                        // CAS guard: dropped at apply if the partition state
                        // moved since ps was read.
                        .setPriorLeaderEpoch(ps.leaderEpoch())
                        .setPriorPartitionEpoch(ps.partitionEpoch())
                        .build();
                var payload = jbroker.proto.raft.MetadataRecord.newBuilder()
                        .setPartitionChange(record)
                        .build()
                        .toByteArray();
                try {
                    raftDriver.propose(payload);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                log.info(
                        "drain: {}-{} → broker {} (epoch {})",
                        proposal.topic(),
                        proposal.partition(),
                        proposal.newLeader(),
                        proposal.newLeaderEpoch());
            }
            // Await this round's applies before re-deriving.
            long roundDeadline = Math.min(deadline, System.currentTimeMillis() + 2_000);
            while (System.currentTimeMillis() < roundDeadline) {
                if (jbroker.broker.controller.LeadershipDrainer.proposeDrain(topicManager, selfBrokerId)
                        .isEmpty()) {
                    break;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        int remaining = jbroker.broker.controller.LeadershipDrainer.proposeDrain(topicManager, selfBrokerId)
                .size();
        if (remaining > 0) {
            log.warn("leadership drain timed out with {} partition(s) still led locally", remaining);
        } else {
            log.info("leadership drain complete after {} round(s)", rounds);
        }
    }

    public void closeAbruptly() {
        isrTicker.shutdownNow();
        registrationTicker.shutdownNow();
        fencerTicker.shutdownNow();
        balancerTicker.shutdownNow();
        diskHeadroom.close();
        // AutoCloseable, not the Redis type: a single-op quota config wraps
        // the Redis enforcer in a gate, and the socket must close either way.
        if (quotaEnforcer instanceof AutoCloseable closeableQuota) {
            try {
                closeableQuota.close();
            } catch (Exception ignored) {
                // best-effort socket release during shutdown
            }
        }
        brokerServer.shutdownNow();
        raftDriver.close();
        heartbeatSender.close();
        fetcherManager.close();
        fetcherFactory.close();
        txnRuntime.close();
        txnMarkerClients.close();
        try {
            logManager.close();
        } catch (IOException ignored) {
            /* best-effort */
        }
    }

    @Override
    public void close() {
        // Shut the tickers down first and wait briefly so an in-flight
        // propose()/fut.get() chain can't race against raftDriver.close().
        isrTicker.shutdownNow();
        registrationTicker.shutdownNow();
        fencerTicker.shutdownNow();
        balancerTicker.shutdownNow();
        try {
            isrTicker.awaitTermination(1, TimeUnit.SECONDS);
            registrationTicker.awaitTermination(1, TimeUnit.SECONDS);
            fencerTicker.awaitTermination(1, TimeUnit.SECONDS);
            balancerTicker.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (chaosHttp != null) chaosHttp.close();
        diskHeadroom.close();
        // AutoCloseable, not the Redis type: a single-op quota config wraps
        // the Redis enforcer in a gate, and the socket must close either way.
        if (quotaEnforcer instanceof AutoCloseable closeableQuota) {
            try {
                closeableQuota.close();
            } catch (Exception ignored) {
                // best-effort socket release during shutdown
            }
        }
        // Stop heartbeat sender before its peer channels race against
        // the peer brokers shutting down their gRPC servers.
        heartbeatSender.close();
        // Stop in-flight fetchers before raftDriver + logManager close so
        // their pollOnce calls can't race against closed resources.
        fetcherManager.close();
        fetcherFactory.close();
        brokerServer.shutdown();
        try {
            brokerServer.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        raftDriver.close();
        // After the server stops taking Txn RPCs and before the log closes:
        // stop marker delivery tasks and release peer channels.
        txnRuntime.close();
        txnMarkerClients.close();
        try {
            logManager.close();
        } catch (IOException ignored) {
            /* best-effort */
        }
    }

    /**
     * Adapts the live {@link RaftDriver} to the minimal membership view the
     * {@link jbroker.broker.controller.MembershipController} orchestrates over:
     * leadership, the voter/learner sets, and leader-side replication progress
     * (leader last index + per-peer matchIndex) that gates learner catch-up.
     */
    private record RaftMembershipAdapter(RaftDriver raft)
            implements jbroker.broker.controller.MembershipController.RaftMembershipAccess {
        @Override
        public boolean isLeader() {
            return raft.role() == jbroker.raft.core.Role.LEADER;
        }

        @Override
        public List<NodeId> voters() {
            return raft.activeVoters();
        }

        @Override
        public List<NodeId> learners() {
            return raft.activeLearners();
        }

        @Override
        public long leaderLastIndex() {
            return raft.replicationProgress().leaderLastIndex();
        }

        @Override
        public java.util.OptionalLong matchIndex(NodeId peer) {
            Long m = raft.replicationProgress().matchByPeer().get(peer);
            return m == null ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(m);
        }

        @Override
        public void proposeMembership(jbroker.raft.core.Membership membership) throws InterruptedException {
            raft.proposeMembership(membership);
        }
    }
}
