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
import jbroker.raft.core.Role;
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

    public record Config(NodeId selfId, Path dataDir, int raftPort, int brokerPort) {}

    private final TopicManager topicManager;
    private final LogManager logManager;
    private final WaitingStateMachine waitingSm;
    private final RaftDriver raftDriver;
    private final Server brokerServer;
    private final int brokerPort;
    private final ScheduledExecutorService isrTicker;

    private Broker(
            TopicManager tm,
            LogManager lm,
            WaitingStateMachine wsm,
            RaftDriver raftDriver,
            Server brokerServer,
            int brokerPort,
            ScheduledExecutorService isrTicker) {
        this.topicManager = tm;
        this.logManager = lm;
        this.waitingSm = wsm;
        this.raftDriver = raftDriver;
        this.brokerServer = brokerServer;
        this.brokerPort = brokerPort;
        this.isrTicker = isrTicker;
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
        var raftConfig = new RaftConfig(
                config.selfId(),
                List.of(config.selfId()),
                TimeUnit.MILLISECONDS.toNanos(500),
                TimeUnit.MILLISECONDS.toNanos(250),
                TimeUnit.MILLISECONDS.toNanos(100),
                100);
        var core = new DefaultRaftCore(raftConfig, raftLog, state, Long.MAX_VALUE);

        var topicManager = new TopicManager();
        var producerIdRegistry = new ProducerIdRegistry();
        var metadataSm = new MetadataStateMachine(topicManager, producerIdRegistry);
        var waitingSm = new WaitingStateMachine(metadataSm);

        // --- LogManager (partition data logs) ---
        var logManager = new LogManager(
                topicsDir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE /* no auto-retention in Phase 5 */,
                        jbroker.storage.LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        TimeUnit.MINUTES.toMillis(5)));

        // --- Raft transport (self-only, empty peer map) ---
        var raftDriver = new RaftDriver(
                config.selfId(), core, waitingSm, Map.<NodeId, RaftPeerClient>of(), TimeUnit.MILLISECONDS.toNanos(30));
        raftDriver.start(config.raftPort());

        // --- Broker gRPC server ---
        var produce =
                new ProduceHandler(logManager, topicManager, config.selfId().value());
        var fetch = new FetchHandler(logManager, topicManager);
        var followerTracker = new FollowerStateTracker();
        var replicaFetch = new ReplicaFetchHandler(
                logManager, topicManager, config.selfId().value(), followerTracker, System::currentTimeMillis);
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
                .addService(BrokerGrpcServices.replicaConsumer(replicaFetch))
                .addService(BrokerGrpcServices.admin(admin))
                .build()
                .start();

        // Single-node Raft elects itself as leader within one election
        // timeout. Spin until the role settles so the first CreateTopic RPC
        // doesn't race with election completion and get dropped as
        // RejectClientPropose.
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline && raftDriver.role() != Role.LEADER) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

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

        return new Broker(topicManager, logManager, waitingSm, raftDriver, server, server.getPort(), isrTicker);
    }

    public int brokerPort() {
        return brokerPort;
    }

    public TopicManager topics() {
        return topicManager;
    }

    @Override
    public void close() {
        // Shut the ticker down first and wait briefly so an in-flight
        // propose()/fut.get() chain can't race against raftDriver.close().
        isrTicker.shutdownNow();
        try {
            isrTicker.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
