package jbroker.admin.dto;

import java.util.List;

/**
 * Cluster-wide chaos state snapshot — what the admin UI polls to paint
 * the topology. Brokers whose cooperative chaos endpoint is disabled or
 * unreachable return {@code available=false}; the UI should render them
 * as "chaos-off" rather than implying the rest of the state is stale.
 */
public record ChaosStateSnapshot(List<BrokerChaosState> brokers) {
    public ChaosStateSnapshot {
        brokers = List.copyOf(brokers);
    }

    /**
     * Per-broker chaos snapshot. {@code available=false} means we couldn't
     * reach the broker's chaos endpoint (chaos disabled, network, etc.);
     * in that case every other field is zero/empty and meaningless.
     */
    public record BrokerChaosState(
            int brokerId,
            boolean available,
            boolean paused,
            List<Integer> outboundBlockedPeers,
            List<Integer> inboundBlockedPeers,
            long latencyMs) {
        public BrokerChaosState {
            outboundBlockedPeers = List.copyOf(outboundBlockedPeers);
            inboundBlockedPeers = List.copyOf(inboundBlockedPeers);
        }

        public static BrokerChaosState unavailable(int brokerId) {
            return new BrokerChaosState(brokerId, false, false, List.of(), List.of(), 0L);
        }
    }
}
