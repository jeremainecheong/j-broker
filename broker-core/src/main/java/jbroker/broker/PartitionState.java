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
 * <p>{@code leaderEpoch} increases strictly on every leader transition;
 * older records arriving out of order are ignored by
 * {@link TopicManager#onPartitionChange}.
 */
public record PartitionState(int leader, List<Integer> isr, List<Integer> replicas, int leaderEpoch) {
    public PartitionState {
        isr = List.copyOf(isr);
        replicas = List.copyOf(replicas);
    }
}
