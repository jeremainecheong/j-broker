package jbroker.broker;

/**
 * Flat (topic, partition, state) triple — the iterable shape exposed by
 * {@link TopicManager#allPartitionAssignments()} for snapshot writers.
 */
public record PartitionAssignment(String topic, int partition, PartitionState state) {}
