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

    /**
     * Called by the state machine when a {@code TopicRecord} commits. Idempotent —
     * re-applying the same record is a no-op.
     */
    public void onTopicCommitted(String topic, int partitions, int replicationFactor, long createdMillis) {
        topics.putIfAbsent(topic, new TopicDescription(topic, partitions, replicationFactor, createdMillis));
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
}
