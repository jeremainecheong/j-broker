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

    /** Inter-broker dial target + optional externally-reachable advertised target (). */
    private record Entry(HostPort internal, HostPort advertised) {}

    private final ConcurrentHashMap<Integer, Entry> entries = new ConcurrentHashMap<>();

    /** Pre-signature kept for call sites that only know an inter-broker address. */
    public void onBrokerRegistration(int brokerId, String host, int port) {
        onBrokerRegistration(brokerId, host, port, "", 0);
    }

    @Override
    public void onBrokerRegistration(int brokerId, String host, int port, String advertisedHost, int advertisedPort) {
        var internal = new HostPort(host, port);
        HostPort advertised;
        if (advertisedHost == null || advertisedHost.isEmpty() || advertisedPort <= 0) {
            advertised = internal;
        } else {
            advertised = new HostPort(advertisedHost, advertisedPort);
        }
        entries.put(brokerId, new Entry(internal, advertised));
    }

    /** Internal address used by inter-broker RPCs (Raft, ReplicaFetch, heartbeat). */
    public Optional<HostPort> addressFor(int brokerId) {
        var e = entries.get(brokerId);
        return e == null ? Optional.empty() : Optional.of(e.internal());
    }

    /**
     * advertised address used in client-facing replies
     * (FindCoordinator, DescribeCluster). Falls back to the internal
     * address when the broker did not advertise a separate listener —
     * matches earlierbehavior for single-network deployments.
     */
    public Optional<HostPort> advertisedAddressFor(int brokerId) {
        var e = entries.get(brokerId);
        return e == null ? Optional.empty() : Optional.of(e.advertised());
    }

    public Set<Integer> knownBrokerIds() {
        return Set.copyOf(entries.keySet());
    }
}
