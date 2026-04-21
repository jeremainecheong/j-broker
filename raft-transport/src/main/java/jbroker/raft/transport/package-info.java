/**
 * gRPC transport + driver loop for the Raft core.
 *
 * <p>{@link jbroker.raft.transport.RaftDriver} owns the event-pump virtual
 * thread, a periodic ticker, a gRPC server, and a map of peer clients.
 * It translates between Raft events / effects and protobuf messages and
 * is the only place allowed to do IO or read the wall clock — the pure
 * {@code raft-core} layer stays side-effect-free.
 *
 * <p>Outbound AppendEntries and RequestVote RPCs are dispatched on
 * structured virtual threads rather than the pump itself, so a slow peer
 * cannot block the event loop. Responses re-enter the core through the
 * same queue the pump reads from, preserving single-threaded state access.
 */
package jbroker.raft.transport;
