package jbroker.broker;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory view of when each broker last signalled liveness via the
 * {@code BrokerHeartbeat} RPC. Populated by {@link BrokerHeartbeatHandler}
 * on receipt of a heartbeat.
 *
 * <p>Consumed by the {@code BrokerFencer} loop on the active
 * controller: a broker whose {@code lastSignal.wallClockNanos} is older
 * than a staleness threshold is fenced from partition leadership.
 *
 * <p>Signals are last-writer-wins with a wall-clock guard — a late
 * heartbeat with an older timestamp can arrive out of order (RPC reorder
 * or peer clock skew) and must not clobber a fresher entry.
 */
public final class BrokerLiveness {

    public record Signal(long wallClockNanos, long metadataOffset) {}

    /** Supported protocol range a peer advertised on its last heartbeat. */
    public record ProtocolRange(int min, int max) {}

    private final ConcurrentHashMap<Integer, Signal> entries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ProtocolRange> protocolRanges = new ConcurrentHashMap<>();

    public void recordSignal(int brokerId, long metadataOffset, long nowNanos) {
        entries.merge(brokerId, new Signal(nowNanos, metadataOffset), (existing, proposed) -> {
            if (proposed.wallClockNanos() >= existing.wallClockNanos()) return proposed;
            return existing;
        });
    }

    /**
     * Records the protocol range {@code brokerId} advertised. A range is
     * fixed for the life of a broker process, so plain last-writer-wins is
     * fine — the out-of-order wall-clock guard on {@link #recordSignal}
     * has nothing to protect here.
     */
    public void recordProtocolRange(int brokerId, int min, int max) {
        protocolRanges.put(brokerId, new ProtocolRange(min, max));
    }

    public Optional<Signal> lastSignal(int brokerId) {
        return Optional.ofNullable(entries.get(brokerId));
    }

    /** Range from the peer's last heartbeat; empty until one carrying a range arrives. */
    public Optional<ProtocolRange> protocolRange(int brokerId) {
        return Optional.ofNullable(protocolRanges.get(brokerId));
    }

    public Set<Integer> knownBrokerIds() {
        return Set.copyOf(entries.keySet());
    }
}
