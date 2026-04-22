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
 * point-to-point RPCs after the investigation showed that follower
 * calls to {@code raftDriver.propose()} are silently dropped. Point-to-
 * point matches real KRaft's broker heartbeat protocol.
 */
public final class BrokerHeartbeatHandler {

    private final BrokerLiveness liveness;
    private final LongSupplier clockNanos;

    public BrokerHeartbeatHandler(BrokerLiveness liveness, LongSupplier clockNanos) {
        this.liveness = liveness;
        this.clockNanos = clockNanos;
    }

    public BrokerHeartbeatResponse handle(BrokerHeartbeatRequest req) {
        liveness.recordSignal(req.getBrokerId(), req.getCurrentMetadataOffset(), clockNanos.getAsLong());
        return BrokerHeartbeatResponse.newBuilder().build();
    }
}
