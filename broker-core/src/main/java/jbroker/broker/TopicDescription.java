package jbroker.broker;

public record TopicDescription(String topic, int partitions, int replicationFactor, long createdMillis) {}
