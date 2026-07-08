package jbroker.broker;

import java.util.List;

/**
 * In-memory view of a partition's replication state.
 *
 * <p>{@code replicas} is the full set of brokers assigned to host this
 * partition — static under normal operation; only changes on reassignment.
 * {@code isr} is the in-sync subset (replicas whose LEO is close enough to
 * the leader's LEO to vote for commits). An out-of-ISR replica still
 * receives fetches so it can catch up and rejoin.
 *
 * <p>Two epochs are tracked separately:
 * <ul>
 *   <li>{@code leaderEpoch} — increases only on leader change; followers
 *       use it to trigger log truncation via {@code OffsetsForLeaderEpoch}.</li>
 *   <li>{@code partitionEpoch} — increases on any metadata change (ISR
 *       flip, replica reassignment). Resets to 0 on leader change.
 *       Followers can treat a partition-epoch-only bump as "refresh
 *       metadata" rather than "truncate and reconcile."</li>
 * </ul>
 * Conflating the two would cause every ISR shrink/expand to trigger a
 * truncation cycle that isn't actually needed.
 */
public record PartitionState(
        int leader, List<Integer> isr, List<Integer> replicas, int leaderEpoch, int partitionEpoch) {
    public PartitionState {
        isr = List.copyOf(isr);
        replicas = List.copyOf(replicas);
    }
}
