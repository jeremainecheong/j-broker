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

    /** Inter-broker dial target + optional externally-reachable advertised target. */
    private record Entry(HostPort internal, HostPort advertised) {}

    private final ConcurrentHashMap<Integer, Entry> entries = new ConcurrentHashMap<>();

    // Rack labels are self-declared: each broker notes its own at startup
    // and learns its peers' from their heartbeats — the replicated
    // registration record can't carry them because the controller
    // synthesises registrations from static voter config, which has no
    // per-broker rack. Kept beside the address book so placement reads
    // one component.
    private final ConcurrentHashMap<Integer, String> racksByBroker = new ConcurrentHashMap<>();

    /** Older signature kept for call sites that only know an inter-broker address. */
    public void onBrokerRegistration(int brokerId, String host, int port) {
        onBrokerRegistration(brokerId, host, port, "", 0, 0);
    }

    // The registry is an address book for client-facing and replica-fetch
    // RPCs; the Raft peer port is consumed only by the driver's peer map
    // (see Broker's registration listener), so it is accepted and ignored here.
    @Override
    public void onBrokerRegistration(
            int brokerId, String host, int port, String advertisedHost, int advertisedPort, int raftPort) {
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
     * Advertised address used in client-facing replies
     * (FindCoordinator, DescribeCluster). Falls back to the internal
     * address when the broker did not advertise a separate listener —
     * matches original behavior for single-network deployments.
     */
    public Optional<HostPort> advertisedAddressFor(int brokerId) {
        var e = entries.get(brokerId);
        return e == null ? Optional.empty() : Optional.of(e.advertised());
    }

    public Set<Integer> knownBrokerIds() {
        return Set.copyOf(entries.keySet());
    }

    /**
     * Record the rack {@code brokerId} declared for itself. Blank clears —
     * a broker restarted without a rack must not keep its old label.
     */
    public void noteRack(int brokerId, String rack) {
        if (rack == null || rack.isBlank()) {
            racksByBroker.remove(brokerId);
        } else {
            racksByBroker.put(brokerId, rack);
        }
    }

    /** The broker's rack label; empty when it never declared one. */
    public String rackFor(int brokerId) {
        return racksByBroker.getOrDefault(brokerId, "");
    }

    /** Snapshot of every declared rack, {@code broker_id -> rack}. */
    public java.util.Map<Integer, String> racks() {
        return java.util.Map.copyOf(racksByBroker);
    }
}
