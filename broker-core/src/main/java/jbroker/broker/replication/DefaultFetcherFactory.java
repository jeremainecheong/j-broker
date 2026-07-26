package jbroker.broker.replication;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * ReplicaFetcher}, and drives its {@link ReplicaFetcher#pollOnce} from a
 * dedicated virtual-thread loop. Each fetch long-polls the leader
 * ({@code max_wait_ms}), so an idle partition costs one parked RPC instead
 * of a fixed-cadence poll, and a fresh append is fetched the moment the
 * leader's park releases. The returned handle interrupts the loop —
 * cancelling any in-flight parked fetch — and closes the peer client.
 *
 * <p>The current {@code leader_epoch} passed to {@code pollOnce} is resolved
 * dynamically from {@link TopicManager} on each iteration, so a leader
 * change that bumps {@code leader_epoch} is observed on the next fetch
 * without restarting the fetcher.
 *
 * <p>{@code pollIntervalMs} survives as the fallback cadence: the
 * reassignment throttle's empty-budget retry sleeps it, and so does the
 * rolling-upgrade guard — a leader predating {@code max_wait_ms} answers
 * empty instantly, and looping straight into the next fetch would
 * hot-spin, so an empty answer faster than {@link #LEGACY_EMPTY_NANOS}
 * degrades to exactly the old fixed-delay behavior.
 */
public final class DefaultFetcherFactory implements ReplicaFetcherManager.FetcherFactory, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DefaultFetcherFactory.class);

    /** Leader-side park budget requested on every fetch. */
    private static final int FETCH_MAX_WAIT_MS = 500;

    /** Empty answers faster than this mean the leader ignored max_wait_ms. */
    private static final long LEGACY_EMPTY_NANOS = TimeUnit.MILLISECONDS.toNanos(5);

    private static final long ERROR_BACKOFF_START_MS = 50;
    private static final long ERROR_BACKOFF_CAP_MS = 1_000;

    private final int selfBrokerId;
    private final LogManager logManager;
    private final TopicManager topicManager;
    private final long pollIntervalMs;
    private final long fetchTimeoutMs;
    private final TlsConfig tls;
    private final ProducerStateManager producerState;
    private final java.util.function.IntFunction<io.grpc.ClientInterceptor> chaosInterceptorFactory;
    private final Set<Thread> loops = ConcurrentHashMap.newKeySet();
    private volatile FetchThrottle throttle = FetchThrottle.UNTHROTTLED;
    private volatile jbroker.broker.txn.TxnPartitionEpochs txnEpochs;

    public DefaultFetcherFactory(
            int selfBrokerId,
            LogManager logManager,
            TopicManager topicManager,
            long pollIntervalMs,
            long fetchTimeoutMs) {
        this(selfBrokerId, logManager, topicManager, pollIntervalMs, fetchTimeoutMs, TlsConfig.DISABLED);
    }

    /** TLS-aware constructor. Wires the same TLS bundle into each {@link ReplicaPeerClient}. */
    public DefaultFetcherFactory(
            int selfBrokerId,
            LogManager logManager,
            TopicManager topicManager,
            long pollIntervalMs,
            long fetchTimeoutMs,
            TlsConfig tls) {
        this(selfBrokerId, logManager, topicManager, pollIntervalMs, fetchTimeoutMs, tls, null, null);
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
        this(selfBrokerId, logManager, topicManager, pollIntervalMs, fetchTimeoutMs, tls, producerState, null);
    }

    /**
     * Full constructor — combines a {@code ProducerStateManager} (for
     * idempotent dedup across failover) and a per-peer
     * {@code ClientInterceptor} factory (so chaos outbound-partition gating
     * actually fires on replica-fetch RPCs). Null is tolerated for either
     * optional dependency.
     */
    public DefaultFetcherFactory(
            int selfBrokerId,
            LogManager logManager,
            TopicManager topicManager,
            long pollIntervalMs,
            long fetchTimeoutMs,
            TlsConfig tls,
            ProducerStateManager producerState,
            java.util.function.IntFunction<io.grpc.ClientInterceptor> chaosInterceptorFactory) {
        this.selfBrokerId = selfBrokerId;
        this.logManager = logManager;
        this.topicManager = topicManager;
        this.pollIntervalMs = pollIntervalMs;
        this.fetchTimeoutMs = fetchTimeoutMs;
        this.tls = tls == null ? TlsConfig.DISABLED : tls;
        this.producerState = producerState;
        this.chaosInterceptorFactory = chaosInterceptorFactory;
    }

    /**
     * Install a per-fetch byte-budget policy (default: unthrottled). The broker
     * uses this to meter reassignment catch-up fetches. Applied to every
     * fetcher's next poll.
     */
    public void setFetchThrottle(FetchThrottle throttle) {
        this.throttle = throttle == null ? FetchThrottle.UNTHROTTLED : throttle;
    }

    /**
     * Thread the broker's shared transactional epoch floors into every
     * fetcher created from now on (same pattern as the throttle: set once
     * at wiring time, before any fetcher starts). Null keeps the feed off.
     */
    public void setTxnPartitionEpochs(jbroker.broker.txn.TxnPartitionEpochs epochs) {
        this.txnEpochs = epochs;
    }

    @Override
    public ReplicaFetcherManager.FetcherHandle start(
            String topic, int partition, int leaderBrokerId, BrokerRegistry.HostPort leaderAddr) {
        var interceptor = chaosInterceptorFactory == null ? null : chaosInterceptorFactory.apply(leaderBrokerId);
        var client = new ReplicaPeerClient(leaderAddr.host(), leaderAddr.port(), tls, interceptor);
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
        var fetcher = new ReplicaFetcher(logManager, topic, partition, selfBrokerId, peer, producerState, txnEpochs);
        var running = new AtomicBoolean(true);
        var thread = Thread.ofVirtual()
                .name("replica-fetch-" + selfBrokerId + "-" + topic + "-" + partition)
                .unstarted(() -> runLoop(topic, partition, fetcher, running));
        loops.add(thread);
        thread.start();
        return () -> {
            running.set(false);
            // Interrupt cancels an in-flight (possibly leader-parked) fetch
            // RPC immediately — a re-point must not wait out the park.
            thread.interrupt();
            loops.remove(thread);
            client.close();
        };
    }

    private void runLoop(String topic, int partition, ReplicaFetcher fetcher, AtomicBoolean running) {
        long backoffMs = 0;
        while (running.get()) {
            try {
                int maxBytes = throttle.maxBytes(topic, partition);
                // 0 means the reassignment throttle has no budget right
                // now — hold the fixed cadence and re-reserve.
                if (maxBytes <= 0) {
                    Thread.sleep(pollIntervalMs);
                    continue;
                }
                int expectedEpoch = topicManager
                        .partitionState(topic, partition)
                        .map(PartitionState::leaderEpoch)
                        .orElse(0);
                long startNanos = System.nanoTime();
                var result = fetcher.pollOnce(expectedEpoch, maxBytes, FETCH_MAX_WAIT_MS, 1);
                switch (result) {
                        // ADVANCED: more may be waiting. RECONCILED: refetch
                        // with the truncated log. EMPTY: the leader parked for
                        // us, so an immediate re-poll parks again rather than
                        // spinning — except against a pre-long-poll leader
                        // (see the guard).
                    case ADVANCED, RECONCILED -> backoffMs = 0;
                    case EMPTY -> {
                        backoffMs = 0;
                        if (System.nanoTime() - startNanos < LEGACY_EMPTY_NANOS) {
                            Thread.sleep(pollIntervalMs);
                        }
                    }
                    case ERROR -> backoffMs = sleepBackoff(backoffMs);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (IOException e) {
                log.debug("fetch loop for {}-{} failed: {}", topic, partition, e.getMessage());
                if (!sleepBackoffQuietly(backoffMs)) return;
                backoffMs = nextBackoff(backoffMs);
            } catch (Exception e) {
                if (!running.get()) return;
                log.debug("fetch loop for {}-{} errored", topic, partition, e);
                if (!sleepBackoffQuietly(backoffMs)) return;
                backoffMs = nextBackoff(backoffMs);
            }
        }
    }

    private static long nextBackoff(long currentMs) {
        return currentMs <= 0 ? ERROR_BACKOFF_START_MS : Math.min(currentMs * 2, ERROR_BACKOFF_CAP_MS);
    }

    private static long sleepBackoff(long currentMs) throws InterruptedException {
        long next = nextBackoff(currentMs);
        Thread.sleep(next);
        return next;
    }

    /** Backoff sleep for exception paths; false when interrupted (loop must exit). */
    private static boolean sleepBackoffQuietly(long currentMs) {
        try {
            Thread.sleep(nextBackoff(currentMs));
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public void close() {
        for (var t : loops) {
            t.interrupt();
        }
        loops.clear();
    }
}
