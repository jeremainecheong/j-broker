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
import jbroker.broker.FetchHandler;
import jbroker.broker.InitProducerIdHandler;
import jbroker.broker.MetadataStateMachine;
import jbroker.broker.OffsetsForLeaderEpochHandler;
import jbroker.broker.ProduceHandler;
import jbroker.broker.ProducerIdRegistry;
import jbroker.broker.ReplicaFetchHandler;
import jbroker.broker.TopicManager;
import jbroker.broker.WaitingStateMachine;
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
 * <p>Bigger broker clusters (Phase 6+) replace the one-node Raft config with
 * a multi-node config; the data-plane stays the same shape.
 */
public final class Broker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Broker.class);

    public record Config(NodeId selfId, Path dataDir, int raftPort, int brokerPort, List<VoterAddress> voters) {
        public Config {
            voters = List.copyOf(voters);
        }

        /**
         * Back-compat overload: single-voter config where the only voter is self,
         * reachable at 127.0.0.1 on the provided {@code raftPort}. All existing
         * single-broker callers keep working unchanged.
         */
        public Config(NodeId selfId, Path dataDir, int raftPort, int brokerPort) {
            this(
                    selfId,
                    dataDir,
                    raftPort,
                    brokerPort,
                    List.of(new VoterAddress(selfId, "127.0.0.1", raftPort, brokerPort)));
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
    private final jbroker.broker.BrokerRegistry brokerRegistry;
    private final jbroker.broker.replication.ReplicaFetcherManager fetcherManager;
    private final jbroker.broker.replication.DefaultFetcherFactory fetcherFactory;

    private Broker(
            TopicManager tm,
            LogManager lm,
            WaitingStateMachine wsm,
            RaftDriver raftDriver,
            Server brokerServer,
            int brokerPort,
            ScheduledExecutorService isrTicker,
            ScheduledExecutorService registrationTicker,
            jbroker.broker.BrokerRegistry brokerRegistry,
            jbroker.broker.replication.ReplicaFetcherManager fetcherManager,
            jbroker.broker.replication.DefaultFetcherFactory fetcherFactory) {
        this.topicManager = tm;
        this.logManager = lm;
        this.waitingSm = wsm;
        this.raftDriver = raftDriver;
        this.brokerServer = brokerServer;
        this.brokerPort = brokerPort;
        this.isrTicker = isrTicker;
        this.registrationTicker = registrationTicker;
        this.brokerRegistry = brokerRegistry;
        this.fetcherManager = fetcherManager;
        this.fetcherFactory = fetcherFactory;
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
                        Long.MAX_VALUE /* no auto-retention in Phase 5 */,
                        jbroker.storage.LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        TimeUnit.MINUTES.toMillis(5)));

        // P6.4: whenever a partition's leader_epoch bumps and self is the
        // new leader, record (epoch, current_leo) in the partition's
        // LeaderEpochCheckpoint so OffsetsForLeaderEpoch can answer.
        int selfId = config.selfId().value();
        MetadataStateMachine.LeaderEpochListener leaderEpochListener = (topic, partition, epoch, leaderId) -> {
            if (leaderId != selfId) return;
            try {
                var leo = logManager.logFor(topic, partition).nextOffset();
                logManager.leaderEpochCheckpoint(topic, partition).assign(epoch, leo);
            } catch (IOException e) {
                log.warn("failed to record leader-epoch checkpoint for {}-{}", topic, partition, e);
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
        };
        var metadataSm = new MetadataStateMachine(
                topicManager, producerIdRegistry, leaderEpochListener, regChain, fetcherManager);
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
        var produce =
                new ProduceHandler(logManager, topicManager, config.selfId().value());
        var fetch = new FetchHandler(logManager, topicManager);
        var followerTracker = new FollowerStateTracker();
        var replicaFetch = new ReplicaFetchHandler(
                logManager, topicManager, config.selfId().value(), followerTracker, System::currentTimeMillis);
        var offsetsForLeaderEpoch = new OffsetsForLeaderEpochHandler(
                logManager, topicManager, config.selfId().value());
        AdminHandler.MetadataProposer proposer = (payload, timeoutMs) -> {
            var fut = waitingSm.awaitApply(payload);
            raftDriver.propose(payload);
            fut.get(timeoutMs, TimeUnit.MILLISECONDS);
        };
        var admin = new AdminHandler(topicManager, proposer, config.selfId().value());
        var initProducerId = new InitProducerIdHandler(producerIdRegistry, proposer);

        var server = NettyServerBuilder.forPort(config.brokerPort())
                .addService(BrokerGrpcServices.producer(produce, initProducerId))
                .addService(BrokerGrpcServices.consumer(fetch))
                .addService(BrokerGrpcServices.replicaConsumer(replicaFetch, offsetsForLeaderEpoch))
                .addService(BrokerGrpcServices.admin(admin))
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
        var registrationTicker = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "broker-registration-" + selfIdVal);
            t.setDaemon(true);
            return t;
        });
        registrationTicker.scheduleWithFixedDelay(
                () -> {
                    if (raftDriver.role() != jbroker.raft.core.Role.LEADER) return;
                    for (var v : config.voters()) {
                        int bid = v.id().value();
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

        return new Broker(
                topicManager,
                logManager,
                waitingSm,
                raftDriver,
                server,
                server.getPort(),
                isrTicker,
                registrationTicker,
                brokerRegistry,
                fetcherManager,
                fetcherFactory);
    }

    public jbroker.broker.BrokerRegistry brokerRegistry() {
        return brokerRegistry;
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

    @Override
    public void close() {
        // Shut the tickers down first and wait briefly so an in-flight
        // propose()/fut.get() chain can't race against raftDriver.close().
        isrTicker.shutdownNow();
        registrationTicker.shutdownNow();
        try {
            isrTicker.awaitTermination(1, TimeUnit.SECONDS);
            registrationTicker.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
