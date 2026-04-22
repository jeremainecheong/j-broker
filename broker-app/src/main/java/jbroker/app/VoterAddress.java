package jbroker.app;

import jbroker.raft.core.NodeId;

/**
 * Static voter identity for the metadata-Raft cluster. {@code host} + {@code
 * port} locate the peer's Raft gRPC endpoint; {@code id} is the same identity
 * used everywhere else (broker_id == NodeId.value()).
 *
 * <p>Voters are static in j-broker (PRD §12.2) — adding or removing a voter
 * requires restart. Broker-gRPC addresses, in contrast, come from {@code
 * BrokerRegistrationRecord} and can change between restarts.
 */
public record VoterAddress(NodeId id, String raftHost, int raftPort) {}
