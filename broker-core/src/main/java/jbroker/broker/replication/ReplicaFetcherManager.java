package jbroker.broker.replication;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import jbroker.broker.BrokerRegistry;
import jbroker.broker.MetadataStateMachine;
import jbroker.broker.PartitionState;
import jbroker.broker.TopicManager;
import jbroker.storage.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns a {@code (topic, partition) -> ReplicaFetcher} map and reconciles it
 * idempotently against {@link TopicManager} + {@link BrokerRegistry} state.
 *
 * <p>For every partition where {@code self} appears in {@code replicas} but
 * isn't the {@code leader} AND the leader's address is known, a fetcher is
 * active. Otherwise, no fetcher is active. {@link #reconcile()} is safe to
 * call on every metadata event — the "no change" case is a cheap no-op.
 *
 * <p>Fetcher construction is delegated to a {@link FetcherFactory} so tests
 * can substitute lightweight stubs for the real {@link ReplicaPeerClient} +
 * {@link ReplicaFetcher} + scheduled pump.
 */
public final class ReplicaFetcherManager
        implements AutoCloseable,
                MetadataStateMachine.PartitionChangeListener,
                MetadataStateMachine.BrokerRegistrationListener {

    private static final Logger log = LoggerFactory.getLogger(ReplicaFetcherManager.class);

    /** Returned by a started fetcher so the manager can stop it. */
    @FunctionalInterface
    public interface FetcherHandle {
        void stop();
    }

    @FunctionalInterface
    public interface FetcherFactory {
        FetcherHandle start(String topic, int partition, int leaderBrokerId, BrokerRegistry.HostPort leaderAddr);
    }

    private final int selfId;
    private final TopicManager topicManager;
    private final BrokerRegistry brokerRegistry;
    private final LogManager logManager;
    private final FetcherFactory factory;

    private record Key(String topic, int partition) {}

    private final ConcurrentHashMap<Key, FetcherHandle> active = new ConcurrentHashMap<>();
    private final ExecutorService reconcileExecutor;
    private final AtomicBoolean pending = new AtomicBoolean();

    public ReplicaFetcherManager(
            int selfId,
            TopicManager topicManager,
            BrokerRegistry brokerRegistry,
            LogManager logManager,
            FetcherFactory factory) {
        this.selfId = selfId;
        this.topicManager = topicManager;
        this.brokerRegistry = brokerRegistry;
        this.logManager = logManager;
        this.factory = factory;
        this.reconcileExecutor = Executors.newSingleThreadExecutor(r -> {
            var t = new Thread(r, "replica-fetcher-reconcile-" + selfId);
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Idempotent: aligns active fetchers to "every partition where self is a
     * follower and the leader's address is known."
     */
    public synchronized void reconcile() {
        var desired = new HashSet<Key>();
        for (var assignment : topicManager.allPartitionAssignments()) {
            var state = assignment.state();
            if (!shouldFollow(state)) continue;
            var addr = brokerRegistry.addressFor(state.leader());
            if (addr.isEmpty()) {
                // Leader address not yet registered — stay pending; a
                // subsequent onBrokerRegistration() reconcile will pick it up.
                continue;
            }
            var key = new Key(assignment.topic(), assignment.partition());
            desired.add(key);
            active.computeIfAbsent(key, k -> {
                log.info("starting replica fetcher for {}-{} -> broker {}", k.topic(), k.partition(), state.leader());
                return factory.start(k.topic(), k.partition(), state.leader(), addr.get());
            });
        }
        active.entrySet().removeIf(e -> {
            if (desired.contains(e.getKey())) return false;
            log.info(
                    "stopping replica fetcher for {}-{}",
                    e.getKey().topic(),
                    e.getKey().partition());
            e.getValue().stop();
            return true;
        });
    }

    private boolean shouldFollow(PartitionState state) {
        return state.leader() != selfId && state.replicas().contains(selfId);
    }

    /**
     * Coalescing async trigger used by listener callbacks. Safe to call from
     * the Raft apply thread — the actual reconcile runs on a dedicated
     * single-thread executor. Multiple concurrent calls collapse to at most
     * one queued reconcile plus at most one running one (coalescing via
     * the {@code pending} flag).
     */
    public void scheduleReconcile() {
        if (reconcileExecutor.isShutdown()) return;
        if (!pending.compareAndSet(false, true)) return;
        try {
            reconcileExecutor.submit(() -> {
                pending.set(false);
                try {
                    reconcile();
                } catch (Exception e) {
                    log.warn("async reconcile failed", e);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Executor shut down between our isShutdown check and submit —
            // nothing to do; close() takes over.
            pending.set(false);
        }
    }

    @Override
    public void onPartitionChange(String topic, int partition, PartitionState state) {
        scheduleReconcile();
    }

    @Override
    public void onBrokerRegistration(int brokerId, String host, int port) {
        scheduleReconcile();
    }

    @Override
    public void close() {
        reconcileExecutor.shutdownNow();
        try {
            reconcileExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        synchronized (this) {
            active.values().forEach(FetcherHandle::stop);
            active.clear();
        }
    }
}
