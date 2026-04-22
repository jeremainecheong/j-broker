package jbroker.broker;

/**
 * Catalogue entry for a single topic. Milestone 7 added {@code internal} (hides
 * the topic from {@code Admin.ListTopics}) and {@code compact} (marks the
 * topic for read-time compaction during recovery walks). Both default to
 * false for backward compatibility — pre-Milestone 7 topics keep their classic
 * non-internal, non-compact behaviour.
 */
public record TopicDescription(
        String topic, int partitions, int replicationFactor, long createdMillis, boolean internal, boolean compact) {

    /** Back-compat constructor: defaults internal and compact to false. */
    public TopicDescription(String topic, int partitions, int replicationFactor, long createdMillis) {
        this(topic, partitions, replicationFactor, createdMillis, false, false);
    }
}
