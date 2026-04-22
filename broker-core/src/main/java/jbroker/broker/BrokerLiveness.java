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

    private final ConcurrentHashMap<Integer, Signal> entries = new ConcurrentHashMap<>();

    public void recordSignal(int brokerId, long metadataOffset, long nowNanos) {
        entries.merge(brokerId, new Signal(nowNanos, metadataOffset), (existing, proposed) -> {
            if (proposed.wallClockNanos() >= existing.wallClockNanos()) return proposed;
            return existing;
        });
    }

    public Optional<Signal> lastSignal(int brokerId) {
        return Optional.ofNullable(entries.get(brokerId));
    }

    public Set<Integer> knownBrokerIds() {
        return Set.copyOf(entries.keySet());
    }
}
