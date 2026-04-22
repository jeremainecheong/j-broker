package jbroker.broker;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory view of {@code broker_id -> host:port} populated by
 * {@link MetadataStateMachine.BrokerRegistrationListener}. Used by
 * {@link jbroker.broker.replication.ReplicaFetcherManager} to resolve a
 * partition leader's data-plane address without consulting static config.
 *
 * <p>Re-registration under the same {@code broker_id} overwrites the
 * previous entry — brokers may legitimately restart with a new port. Readers
 * may observe an in-flight overwrite; the worst case is one wasted fetcher
 * reconnect when the value settles a moment later.
 */
public final class BrokerRegistry implements MetadataStateMachine.BrokerRegistrationListener {

    public record HostPort(String host, int port) {}

    private final ConcurrentHashMap<Integer, HostPort> entries = new ConcurrentHashMap<>();

    @Override
    public void onBrokerRegistration(int brokerId, String host, int port) {
        entries.put(brokerId, new HostPort(host, port));
    }

    public Optional<HostPort> addressFor(int brokerId) {
        return Optional.ofNullable(entries.get(brokerId));
    }

    public Set<Integer> knownBrokerIds() {
        return Set.copyOf(entries.keySet());
    }
}
