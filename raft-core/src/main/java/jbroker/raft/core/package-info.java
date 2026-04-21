/**
 * Pure Raft core: deterministic step-function over {@link jbroker.raft.core.RaftEvent}
 * producing {@link jbroker.raft.core.RaftEffect}. No threads, no IO, no clock
 * dependencies; all time flows in through {@code Tick} events and all persistence
 * flows out through {@code PersistLog} / {@code PersistState} effects.
 *
 * <p>Module boundary (enforced by {@link jbroker.raft.core.ModuleBoundaryTest}):
 * this package must not reference gRPC, Netty, Spring, or Jakarta — any
 * dependency on those belongs in {@code raft-transport} or higher layers.
 * Adding such a dependency here will fail the ArchUnit test.
 *
 * <p>Phase 3 will reuse this determinism to drive a deterministic simulator
 * for randomised / jepsen-style testing; preserving the zero-side-effect
 * property of {@link jbroker.raft.core.RaftCore#step} is load-bearing.
 */
package jbroker.raft.core;
