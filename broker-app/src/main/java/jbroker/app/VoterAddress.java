package jbroker.app;

import jbroker.raft.core.NodeId;

/**
 * Static voter identity for the metadata-Raft cluster. {@code host} is the
 * hostname used for both Raft peer RPCs (at {@code raftPort}) and broker
 * data-plane RPCs (at {@code brokerPort}); {@code id} is the same identity
 * used everywhere else (broker_id == NodeId.value()).
 *
 * <p>Voters are static in j-broker (PRD §12.2) — adding or removing a voter
 * requires restart. {@code BrokerRegistrationRecord}s propagate the
 * broker-gRPC address into every broker's {@code BrokerRegistry} so
 * follower fetchers can dial peers without consulting this static config.
 *
 * <p>P15.1 — {@code advertisedHost} / {@code advertisedBrokerPort} override
 * the values surfaced in client-facing {@code FindCoordinator} and
 * {@code DescribeCluster} replies so external clients can reach the broker
 * via its published port (Docker, K8s NodePort, ...). When unset, falls
 * back to {@code host} / {@code brokerPort} — preserves the single-network
 * behavior the existing ITs depend on.
 */
public record VoterAddress(
        NodeId id, String host, int raftPort, int brokerPort, String advertisedHost, int advertisedBrokerPort) {

    public VoterAddress(NodeId id, String host, int raftPort, int brokerPort) {
        this(id, host, raftPort, brokerPort, "", 0);
    }

    public boolean hasAdvertised() {
        return advertisedHost != null && !advertisedHost.isEmpty() && advertisedBrokerPort > 0;
    }

    /** Effective host surfaced to clients: advertised when set, else internal. */
    public String effectiveAdvertisedHost() {
        return hasAdvertised() ? advertisedHost : host;
    }

    /** Effective port surfaced to clients: advertised when set, else internal. */
    public int effectiveAdvertisedPort() {
        return hasAdvertised() ? advertisedBrokerPort : brokerPort;
    }
}
