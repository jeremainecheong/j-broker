package jbroker.broker;

import java.util.function.LongSupplier;
import jbroker.proto.broker.BrokerHeartbeatRequest;
import jbroker.proto.broker.BrokerHeartbeatResponse;

/**
 * Receives {@code BrokerHeartbeat} RPCs from peer brokers and updates the
 * local {@link BrokerLiveness} map. Every broker runs one of these; each
 * has an independent view of the cluster's liveness.
 *
 * <p>Heartbeats do NOT go through Raft — the design pivoted to
 * point-to-point RPCs after an investigation showed that follower
 * calls to {@code raftDriver.propose()} are silently dropped. Point-to-
 * point matches real KRaft's broker heartbeat protocol.
 */
public final class BrokerHeartbeatHandler {

    private final BrokerLiveness liveness;
    private final LongSupplier clockNanos;
    private final java.util.function.BiConsumer<Integer, String> rackSink;

    public BrokerHeartbeatHandler(BrokerLiveness liveness, LongSupplier clockNanos) {
        this(liveness, clockNanos, null);
    }

    /**
     * Rack-aware constructor: {@code rackSink} (typically
     * {@link BrokerRegistry#noteRack}) receives the rack label each peer
     * declares on its heartbeat, blank included so a peer restarted
     * without a rack sheds its old label.
     */
    public BrokerHeartbeatHandler(
            BrokerLiveness liveness, LongSupplier clockNanos, java.util.function.BiConsumer<Integer, String> rackSink) {
        this.liveness = liveness;
        this.clockNanos = clockNanos;
        this.rackSink = rackSink;
    }

    public BrokerHeartbeatResponse handle(BrokerHeartbeatRequest req) {
        liveness.recordSignal(req.getBrokerId(), req.getCurrentMetadataOffset(), clockNanos.getAsLong());
        // max == 0 means the sender predates protocol-version discovery —
        // leave the peer's range unknown rather than record a bogus 0/0.
        if (req.getSupportedProtocolMax() > 0) {
            liveness.recordProtocolRange(
                    req.getBrokerId(), req.getSupportedProtocolMin(), req.getSupportedProtocolMax());
        }
        if (rackSink != null) {
            rackSink.accept(req.getBrokerId(), req.getRack());
        }
        return BrokerHeartbeatResponse.newBuilder().build();
    }
}
