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
 */
public record VoterAddress(NodeId id, String host, int raftPort, int brokerPort) {}
