package jbroker.broker.replication;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
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

    @Override
    public void onPartitionChange(String topic, int partition, PartitionState state) {
        reconcile();
    }

    @Override
    public void onBrokerRegistration(int brokerId, String host, int port) {
        reconcile();
    }

    @Override
    public synchronized void close() {
        active.values().forEach(FetcherHandle::stop);
        active.clear();
    }
}
