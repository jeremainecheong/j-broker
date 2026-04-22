package jbroker.broker.group;

import java.util.List;
import java.util.Map;
import java.util.Set;
import jbroker.proto.common.TopicPartition;

/**
 * Server-side strategy that maps consumer-group members to partitions.
 * Milestone 7 ships two: {@link RangeAssignor} (Kafka default — contiguous
 * runs) and {@link UniformAssignor} (round-robin — even load when topics
 * share members).
 *
 * <p>All assignors are deterministic: the same inputs (caller pre-sorted)
 * produce the same outputs across JVM runs. This is what makes
 * {@link GroupCoordinator} reproducible under test and predictable across
 * coordinator failover.
 *
 * <p>Caller contract: {@code sortedMemberIds} is in ascending UTF-16 order,
 * {@code subscriptionsByMember} maps every member id to its subscription
 * set, and {@code partitionsByTopic} covers every topic mentioned in any
 * subscription.
 */
public interface PartitionAssignor {

    /** Identifier embedded in {@code GroupMetadataValue.protocol}. */
    String name();

    /**
     * Compute the assignment. Output map covers every member, including
     * those that get an empty list. Within each member's list, partitions
     * are sorted by {@code (topic, partition)} ascending so equality
     * checks in tests are stable.
     */
    Map<String, List<TopicPartition>> assign(
            List<String> sortedMemberIds,
            Map<String, Set<String>> subscriptionsByMember,
            Map<String, Integer> partitionsByTopic);
}
