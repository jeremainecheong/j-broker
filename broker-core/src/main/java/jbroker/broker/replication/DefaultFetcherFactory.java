package jbroker.broker.replication;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import jbroker.broker.BrokerRegistry;
import jbroker.broker.PartitionState;
import jbroker.broker.ProducerStateManager;
import jbroker.broker.TopicManager;
import jbroker.proto.broker.ReplicaFetchRequest;
import jbroker.proto.broker.ReplicaFetchResponse;
import jbroker.storage.LogManager;
import jbroker.tls.TlsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Production {@link ReplicaFetcherManager.FetcherFactory}: for each follower
 * partition it opens a {@link ReplicaPeerClient}, constructs a {@link
 * ReplicaFetcher}, and schedules its {@link ReplicaFetcher#pollOnce} on a
 * shared pump at {@code pollIntervalMs} cadence. The returned handle stops
 * the schedule and closes the peer client.
 *
 * <p>The current {@code leader_epoch} passed to {@code pollOnce} is resolved
 * dynamically from {@link TopicManager} on each tick, so a leader change
 * that bumps {@code leader_epoch} is observed on the next tick without
 * restarting the fetcher.
 */
public final class DefaultFetcherFactory implements ReplicaFetcherManager.FetcherFactory, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DefaultFetcherFactory.class);

    private final int selfBrokerId;
    private final LogManager logManager;
    private final TopicManager topicManager;
    private final ScheduledExecutorService pump;
    private final long pollIntervalMs;
    private final long fetchTimeoutMs;
    private final TlsConfig tls;
    private final ProducerStateManager producerState;

    public DefaultFetcherFactory(
            int selfBrokerId,
            LogManager logManager,
            TopicManager topicManager,
            long pollIntervalMs,
            long fetchTimeoutMs) {
        this(selfBrokerId, logManager, topicManager, pollIntervalMs, fetchTimeoutMs, TlsConfig.DISABLED);
    }

    /** P15.2 — TLS-aware constructor. Wires the same TLS bundle into each {@link ReplicaPeerClient}. */
    public DefaultFetcherFactory(
            int selfBrokerId,
            LogManager logManager,
            TopicManager topicManager,
            long pollIntervalMs,
            long fetchTimeoutMs,
            TlsConfig tls) {
        this(selfBrokerId, logManager, topicManager, pollIntervalMs, fetchTimeoutMs, tls, null);
    }

    /**
     * Audit-finding #1 — constructor threading a shared {@link ProducerStateManager}
     * into every {@link ReplicaFetcher} so the follower's replica-fetch apply
     * path keeps dedup state in sync with the leader. Null is tolerated for
     * legacy tests that don't exercise idempotent producers.
     */
    public DefaultFetcherFactory(
            int selfBrokerId,
            LogManager logManager,
            TopicManager topicManager,
            long pollIntervalMs,
            long fetchTimeoutMs,
            TlsConfig tls,
            ProducerStateManager producerState) {
        this.selfBrokerId = selfBrokerId;
        this.logManager = logManager;
        this.topicManager = topicManager;
        this.pollIntervalMs = pollIntervalMs;
        this.fetchTimeoutMs = fetchTimeoutMs;
        this.tls = tls == null ? TlsConfig.DISABLED : tls;
        this.producerState = producerState;
        this.pump = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "replica-fetcher-pump-" + selfBrokerId);
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public ReplicaFetcherManager.FetcherHandle start(
            String topic, int partition, int leaderBrokerId, BrokerRegistry.HostPort leaderAddr) {
        var client = new ReplicaPeerClient(leaderAddr.host(), leaderAddr.port(), tls);
        ReplicaFetcher.Peer peer = new ReplicaFetcher.Peer() {
            @Override
            public ReplicaFetchResponse fetch(ReplicaFetchRequest req) {
                return client.fetch(req, fetchTimeoutMs);
            }

            @Override
            public long offsetsForLeaderEpoch(String t, int p, int e) {
                return client.offsetsForLeaderEpoch(t, p, e, fetchTimeoutMs);
            }
        };
        var fetcher = new ReplicaFetcher(logManager, topic, partition, selfBrokerId, peer, producerState);
        ScheduledFuture<?> task = pump.scheduleWithFixedDelay(
                () -> {
                    try {
                        int expectedEpoch = topicManager
                                .partitionState(topic, partition)
                                .map(PartitionState::leaderEpoch)
                                .orElse(0);
                        fetcher.pollOnce(expectedEpoch);
                    } catch (IOException e) {
                        log.debug("fetcher tick for {}-{} failed: {}", topic, partition, e.getMessage());
                    } catch (Exception e) {
                        log.debug("fetcher tick for {}-{} errored", topic, partition, e);
                    }
                },
                0,
                pollIntervalMs,
                TimeUnit.MILLISECONDS);
        return () -> {
            task.cancel(true);
            client.close();
        };
    }

    @Override
    public void close() {
        pump.shutdownNow();
    }
}
