package jbroker.broker;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory topic catalogue, populated by {@link MetadataStateMachine}
 * replaying applied {@code TopicRecord} entries. Thread-safe for concurrent
 * readers (produce + fetch request paths) while the state machine mutates
 * on the Raft apply thread.
 */
public final class TopicManager {

    private final ConcurrentHashMap<String, TopicDescription> topics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PartitionKey, PartitionState> partitions = new ConcurrentHashMap<>();

    /**
     * Called by the state machine when a {@code TopicRecord} commits. Idempotent —
     * re-applying the same record is a no-op.
     */
    public void onTopicCommitted(String topic, int partitions, int replicationFactor, long createdMillis) {
        topics.putIfAbsent(topic, new TopicDescription(topic, partitions, replicationFactor, createdMillis));
    }

    /**
     * Called by the state machine when a {@code PartitionChangeRecord} commits.
     * Epoch-guarded: a record with an epoch not strictly greater than the
     * currently-applied epoch is ignored, so out-of-order delivery during
     * recovery cannot regress partition state.
     */
    public void onPartitionChange(String topic, int partition, int leader, List<Integer> isr, int leaderEpoch) {
        var key = new PartitionKey(topic, partition);
        var next = new PartitionState(leader, isr, leaderEpoch);
        partitions.merge(
                key,
                next,
                (existing, proposed) -> proposed.leaderEpoch() > existing.leaderEpoch() ? proposed : existing);
    }

    public Optional<TopicDescription> describe(String topic) {
        return Optional.ofNullable(topics.get(topic));
    }

    public List<TopicDescription> list() {
        return List.copyOf(topics.values());
    }

    public boolean exists(String topic) {
        return topics.containsKey(topic);
    }

    public Optional<Integer> partitionLeader(String topic, int partition) {
        return Optional.ofNullable(partitions.get(new PartitionKey(topic, partition)))
                .map(PartitionState::leader);
    }

    public List<Integer> partitionIsr(String topic, int partition) {
        var s = partitions.get(new PartitionKey(topic, partition));
        return s == null ? List.of() : s.isr();
    }

    public Optional<Integer> partitionLeaderEpoch(String topic, int partition) {
        return Optional.ofNullable(partitions.get(new PartitionKey(topic, partition)))
                .map(PartitionState::leaderEpoch);
    }

    /**
     * Snapshot of every known partition's (leader, ISR, epoch), in no
     * particular order. Used by {@link MetadataStateMachine#snapshot} to
     * persist partition state alongside the topic catalogue.
     */
    public List<PartitionAssignment> allPartitionAssignments() {
        var out = new java.util.ArrayList<PartitionAssignment>(partitions.size());
        partitions.forEach((k, v) -> out.add(new PartitionAssignment(k.topic(), k.partition(), v)));
        return List.copyOf(out);
    }

    private record PartitionKey(String topic, int partition) {}
}
