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
     *
     * <p>Back-compat overload: defaults {@code internal} and {@code compact}
     * to {@code false} for pre-Milestone 7 callers + tests.
     */
    public void onTopicCommitted(String topic, int partitions, int replicationFactor, long createdMillis) {
        onTopicCommitted(topic, partitions, replicationFactor, createdMillis, false, false);
    }

    /**
     * Milestone 7 form: also accepts {@code internal} and {@code compact} flags.
     * Topics whose name starts with {@code __} are forced to {@code internal=true}
     * even if the caller passes {@code false}, so {@link #list()} reliably
     * hides them.
     */
    public void onTopicCommitted(
            String topic,
            int partitions,
            int replicationFactor,
            long createdMillis,
            boolean internal,
            boolean compact) {
        boolean effectiveInternal = internal || topic.startsWith("__");
        topics.putIfAbsent(
                topic,
                new TopicDescription(topic, partitions, replicationFactor, createdMillis, effectiveInternal, compact));
    }

    /**
     * Called by the state machine when a {@code PartitionChangeRecord} commits.
     * Accept-if-newer ordering:
     * <ul>
     *   <li>Strictly higher {@code leaderEpoch} always wins (leader change).</li>
     *   <li>Same {@code leaderEpoch} with strictly higher {@code partitionEpoch}
     *       wins (ISR flip or replica reassignment under the same leader).</li>
     *   <li>Otherwise the existing state is kept.</li>
     * </ul>
     */
    public void onPartitionChange(
            String topic,
            int partition,
            int leader,
            List<Integer> isr,
            List<Integer> replicas,
            int leaderEpoch,
            int partitionEpoch) {
        var key = new PartitionKey(topic, partition);
        var next = new PartitionState(leader, isr, replicas, leaderEpoch, partitionEpoch);
        partitions.merge(key, next, (existing, proposed) -> {
            if (proposed.leaderEpoch() > existing.leaderEpoch()) return proposed;
            if (proposed.leaderEpoch() == existing.leaderEpoch()
                    && proposed.partitionEpoch() > existing.partitionEpoch()) {
                return proposed;
            }
            return existing;
        });
    }

    /** Back-compat overload: defaults replicas to isr, partitionEpoch to 0. */
    public void onPartitionChange(String topic, int partition, int leader, List<Integer> isr, int leaderEpoch) {
        onPartitionChange(topic, partition, leader, isr, isr, leaderEpoch, 0);
    }

    /** Back-compat overload: accepts explicit replicas, defaults partitionEpoch to 0. */
    public void onPartitionChange(
            String topic, int partition, int leader, List<Integer> isr, List<Integer> replicas, int leaderEpoch) {
        onPartitionChange(topic, partition, leader, isr, replicas, leaderEpoch, 0);
    }

    public Optional<TopicDescription> describe(String topic) {
        return Optional.ofNullable(topics.get(topic));
    }

    /**
     * User-visible topics — internal topics ({@code __consumer_offsets}, etc.)
     * are filtered out so {@code Admin.ListTopics} doesn't show plumbing.
     */
    public List<TopicDescription> list() {
        var out = new java.util.ArrayList<TopicDescription>();
        for (var t : topics.values()) {
            if (!t.internal()) out.add(t);
        }
        return List.copyOf(out);
    }

    /** All topics, internal and user-visible. */
    public List<TopicDescription> listAll() {
        return List.copyOf(topics.values());
    }

    /** Internal topics only. */
    public List<TopicDescription> listInternal() {
        var out = new java.util.ArrayList<TopicDescription>();
        for (var t : topics.values()) {
            if (t.internal()) out.add(t);
        }
        return List.copyOf(out);
    }

    public boolean exists(String topic) {
        return topics.containsKey(topic);
    }

    public Optional<Integer> partitionLeader(String topic, int partition) {
        return Optional.ofNullable(partitions.get(new PartitionKey(topic, partition)))
                .map(PartitionState::leader);
    }

    /**
     * Atomic view of a partition's current (leader, ISR, leaderEpoch).
     * Callers that need both leader and epoch together should use this
     * accessor instead of combining {@link #partitionLeader} and
     * {@link #partitionLeaderEpoch} — those two calls could otherwise read
     * different generations across a leadership change.
     */
    public Optional<PartitionState> partitionState(String topic, int partition) {
        return Optional.ofNullable(partitions.get(new PartitionKey(topic, partition)));
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
