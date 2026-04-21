package jbroker.broker;

import java.util.List;

/**
 * In-memory view of a partition's replication state, kept in sync with the
 * replicated {@code PartitionChangeRecord}s. The {@code leaderEpoch} increases
 * strictly on every leader transition; older records arriving out of order
 * are ignored by {@link TopicManager#onPartitionChange}.
 *
 * <p>Milestone 5 left this tracking stubbed; Milestone 6.1 introduces the shape.
 */
public record PartitionState(int leader, List<Integer> isr, int leaderEpoch) {
    public PartitionState {
        isr = List.copyOf(isr);
    }
}
