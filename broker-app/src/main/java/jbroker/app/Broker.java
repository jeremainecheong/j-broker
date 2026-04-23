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
 * <p>Bigger broker clusters (Milestone 6+) replace the one-node Raft config with
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
            int consumerOffsetsPartitions) {
        public Config {
            voters = List.copyOf(voters);
            if (consumerOffsetsPartitions < 1) {
                throw new IllegalArgumentException(
                        "consumerOffsetsPartitions must be ≥ 1, got " + consumerOffsetsPartitions);
            }
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
            return new Config(selfId, dataDir, raftPort, brokerPort, voters, n);
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
    private final jbroker.broker.BrokerRegistry brokerRegistry;
    private final jbroker.broker.BrokerLiveness brokerLiveness;
    private final jbroker.broker.BrokerHeartbeatSender heartbeatSender;
    private final jbroker.broker.replication.ReplicaFetcherManager fetcherManager;
    private final jbroker.broker.replication.DefaultFetcherFactory fetcherFactory;
    private final jbroker.broker.BrokerMetrics metrics;

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
            jbroker.broker.BrokerRegistry brokerRegistry,
            jbroker.broker.BrokerLiveness brokerLiveness,
            jbroker.broker.BrokerHeartbeatSender heartbeatSender,
            jbroker.broker.replication.ReplicaFetcherManager fetcherManager,
            jbroker.broker.replication.DefaultFetcherFactory fetcherFactory,
            jbroker.broker.BrokerMetrics metrics) {
        this.topicManager = tm;
        this.logManager = lm;
        this.waitingSm = wsm;
        this.raftDriver = raftDriver;
        this.brokerServer = brokerServer;
        this.brokerPort = brokerPort;
        this.isrTicker = isrTicker;
        this.registrationTicker = registrationTicker;
        this.fencerTicker = fencerTicker;
        this.brokerRegistry = brokerRegistry;
        this.brokerLiveness = brokerLiveness;
        this.heartbeatSender = heartbeatSender;
        this.fetcherManager = fetcherManager;
        this.fetcherFactory = fetcherFactory;
        this.metrics = metrics;
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
        var logManager = new LogManager(
                topicsDir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE /* no auto-retention in Milestone 5 */,
                        jbroker.storage.LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        TimeUnit.MINUTES.toMillis(5)));

        // per-broker event publisher for the Milestone 8 SSE stream.
        // Declared here so the leader-epoch listener (next block) can
        // capture it. Wired into the MetadataServiceHandler at the bottom
        // of start() where the stream RPC reads from it.
        var eventPublisher = new jbroker.broker.events.BrokerEventPublisher();

        // : whenever a partition's leader_epoch bumps and self is the
        // new leader, record (epoch, current_leo) in the partition's
        // LeaderEpochCheckpoint so OffsetsForLeaderEpoch can answer.
        int selfId = config.selfId().value();
        MetadataStateMachine.LeaderEpochListener leaderEpochListener = (topic, partition, epoch, leaderId) -> {
            if (leaderId == selfId) {
                try {
                    var leo = logManager.logFor(topic, partition).nextOffset();
                    logManager.leaderEpochCheckpoint(topic, partition).assign(epoch, leo);
                } catch (IOException e) {
                    log.warn("failed to record leader-epoch checkpoint for {}-{}", topic, partition, e);
                }
            }
            // emit a leader_changed event for admin SSE, regardless
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
        var fetcherFactory = new jbroker.broker.replication.DefaultFetcherFactory(
                config.selfId().value(), logManager, topicManager, /*pollIntervalMs*/ 25L, /*fetchTimeoutMs*/ 2_000L);
        var fetcherManager = new jbroker.broker.replication.ReplicaFetcherManager(
                config.selfId().value(), topicManager, brokerRegistry, logManager, fetcherFactory);
        MetadataStateMachine.BrokerRegistrationListener regChain = (bid, h, pt) -> {
            brokerRegistry.onBrokerRegistration(bid, h, pt);
            fetcherManager.onBrokerRegistration(bid, h, pt);
            long id = eventPublisher.allocateId();
            eventPublisher.publish(new jbroker.broker.events.BrokerEvent.BrokerRegistered(id, bid, h, pt));
        };
        // on DeleteTopic, evict LogManager cache + segment files so
        // topic-recreation with the same name can't pick up stale offsets,
        // and kick the replica-fetcher reconcile loop so any live fetcher
        // stops as soon as the metadata record applies.
        MetadataStateMachine.TopicDeletionListener topicDeletionChain = deletedTopic -> {
            logManager.deleteTopicDir(deletedTopic);
            fetcherManager.scheduleReconcile();
        };
        var metadataSm = new MetadataStateMachine(
                topicManager, producerIdRegistry, leaderEpochListener, regChain, fetcherManager, topicDeletionChain);
        var waitingSm = new WaitingStateMachine(metadataSm);

        // --- Raft transport (peer map built from static voter set) ---
        var peerMap = new java.util.HashMap<NodeId, RaftPeerClient>();
        for (var v : config.voters()) {
            if (v.id().equals(config.selfId())) continue;
            peerMap.put(v.id(), new RaftPeerClient(v.id(), v.host(), v.raftPort()));
        }
        var raftDriver = new RaftDriver(
                config.selfId(), core, waitingSm, Map.copyOf(peerMap), TimeUnit.MILLISECONDS.toNanos(30));
        raftDriver.start(config.raftPort());

        // --- Broker gRPC server ---
        var brokerMetrics = new jbroker.broker.BrokerMetrics();
        var fetchSessionCache = new jbroker.broker.FetchSessionCache();
        var fetch = new FetchHandler(logManager, topicManager, fetchSessionCache, brokerMetrics);
        var followerTracker = new FollowerStateTracker();
        var produce =
                new ProduceHandler(logManager, topicManager, config.selfId().value(), followerTracker, brokerMetrics);
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
        var admin = new AdminHandler(
                topicManager,
                proposer,
                config.selfId().value(),
                brokerRegistry::knownBrokerIds,
                () -> raftDriver.currentLeader().map(jbroker.raft.core.NodeId::value),
                brokerRegistry);
        var initProducerId = new InitProducerIdHandler(producerIdRegistry, proposer);

        var brokerLiveness = new jbroker.broker.BrokerLiveness();
        var heartbeatHandler = new jbroker.broker.BrokerHeartbeatHandler(brokerLiveness, System::nanoTime);

        // Group coordinator: in-memory state for groups whose coordinator
        // partition this broker leads. wires the heartbeat path;         // staticness; will wire the per-coord-partition activation
        // listener so coordinator failover rebuilds state.
        var groupCoordinator = new GroupCoordinator(
                topic -> topicManager.describe(topic).map(td -> td.partitions()).orElse(0), new RangeAssignor());
        // in-memory offset commit/fetch cache. The recovery walk
        // (below, after gRPC server start) re-reads any __consumer_offsets
        // partitions this broker leads after Raft replay finishes;
        // coordinator-failover-time recovery is .
        var offsetCache = new OffsetCache();
        // persist GroupMetadataValue records on every membership
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
        // Metadata service: DescribeCluster now wired to live
        // BrokerRegistry + BrokerLiveness + Raft state. Remaining RPCs
        // return UNIMPLEMENTED until their owning slice lands.
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
                // Milestone 8 scope: metadata_offset is a forward-compat field.
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
        var server = NettyServerBuilder.forPort(config.brokerPort())
                .addService(BrokerGrpcServices.producer(produce, initProducerId))
                .addService(BrokerGrpcServices.consumer(fetch, consumerHandler))
                .addService(BrokerGrpcServices.replicaConsumer(replicaFetch, offsetsForLeaderEpoch))
                .addService(BrokerGrpcServices.cluster(heartbeatHandler))
                .addService(BrokerGrpcServices.admin(admin))
                .addService(BrokerGrpcServices.metadata(metadataHandler))
                .build()
                .start();

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
        // partitions whose offset log has already been replayed into
        // the OffsetCache. Set guards against re-walking on every tick.
        var recoveredPartitions = new java.util.concurrent.ConcurrentHashMap<Integer, Boolean>();
        var registrationTicker = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "broker-registration-" + selfIdVal);
            t.setDaemon(true);
            return t;
        });
        // : __consumer_offsets needs to exist before any consumer-group
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
                                var record = jbroker.proto.raft.MetadataRecord.newBuilder()
                                        .setBroker(jbroker.proto.raft.BrokerRegistrationRecord.newBuilder()
                                                .setBrokerId(bid)
                                                .setHost(v.host())
                                                .setPort(v.brokerPort())
                                                .build())
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
                    }
                    // Recovery is NOT gated on Raft leadership: every broker
                    // must bootstrap its OffsetCache + GroupCoordinator for
                    // __consumer_offsets partitions IT leads locally. After
                    // a coord failover (/ ), the new partition
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
                },
                0,
                1,
                TimeUnit.SECONDS);

        // ISR housekeeping ticker: every 2s, ask IsrManager for any
        // (leader, ISR) flips and propose them through Raft. 10s lag
        // timeout matches Kafka's default replica.lag.time.max.ms.
        var isr = new IsrManager(
                config.selfId().value(), topicManager, logManager, followerTracker, TimeUnit.SECONDS.toMillis(10));
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
                        // drop members past their session timeout.
                        // Runs on every broker (each one is the coordinator
                        // for its own __consumer_offsets partitions) so the
                        // tick is unconditional, not leader-gated.
                        groupCoordinator.tickEvictions(System.nanoTime());
                    } catch (Exception e) {
                        log.debug("group eviction tick failed, retrying on next tick", e);
                    }
                },
                1,
                1,
                TimeUnit.SECONDS);

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
        // 's fencer only looks at wall-clock, so a placeholder 0 is
        // fine here. Wire up a real applied-offset supplier if a consumer
        // needs it.
        // 250ms heartbeat with 3s staleness threshold = 12 missed
        // heartbeats to trigger a false-positive fence — safe under
        // heavy GC / JVM load. Low enough traffic (6 RPCs/s per broker
        // in a 3-broker cluster) to be negligible.
        var heartbeatSender = new jbroker.broker.BrokerHeartbeatSender(
                config.selfId().value(), heartbeatPeers, () -> 0L, /*intervalMs*/ 250L);
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
                brokerRegistry,
                brokerLiveness,
                heartbeatSender,
                fetcherManager,
                fetcherFactory,
                brokerMetrics);
    }

    /** broker-local counter bag (incremental-fetch hits, etc). */
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

    /** Test-only accessor for assertion-driven inspection of per-broker logs. */
    public LogManager logManager() {
        return logManager;
    }

    /**
     * + for each {@code __consumer_offsets} partition this
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
            int offsetsApplied = jbroker.broker.group.OffsetCacheRecovery.rebuild(logManager, p, offsetCache);
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
     * encode and append a single Type-2 record carrying the
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
        jbroker.storage.RecordBatch.encode(
                buf,
                base, /*partitionLeaderEpoch*/
                0,
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
    public void closeAbruptly() {
        isrTicker.shutdownNow();
        registrationTicker.shutdownNow();
        fencerTicker.shutdownNow();
        brokerServer.shutdownNow();
        raftDriver.close();
        heartbeatSender.close();
        fetcherManager.close();
        fetcherFactory.close();
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
        try {
            isrTicker.awaitTermination(1, TimeUnit.SECONDS);
            registrationTicker.awaitTermination(1, TimeUnit.SECONDS);
            fencerTicker.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
        try {
            logManager.close();
        } catch (IOException ignored) {
            /* best-effort */
        }
    }
}
