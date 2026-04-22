package jbroker.broker.group;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import jbroker.proto.common.TopicPartition;

/**
 * Round-robin assignor: pool all (topic, partition) pairs across all
 * subscribed topics, sort by (topic, partition), then deal one-by-one to
 * the subscribed members in id order. Even load when topics share members;
 * preferred over Range when partition counts are uneven across topics.
 *
 * <p>Per-pair eligibility: a member receives a (topic, partition) only if
 * it subscribes to {@code topic}. The dealer skips members for which the
 * candidate topic isn't subscribed and rotates to the next.
 */
public final class UniformAssignor implements PartitionAssignor {

    public static final String NAME = "uniform";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Map<String, List<TopicPartition>> assign(
            List<String> sortedMemberIds,
            Map<String, Set<String>> subscriptionsByMember,
            Map<String, Integer> partitionsByTopic) {
        var out = new LinkedHashMap<String, List<TopicPartition>>();
        for (var m : sortedMemberIds) {
            out.put(m, new ArrayList<>());
        }
        var allPairs = new ArrayList<TopicPartition>();
        var sortedTopics = new ArrayList<>(partitionsByTopic.keySet());
        sortedTopics.sort(Comparator.naturalOrder());
        for (var topic : sortedTopics) {
            int partitions = partitionsByTopic.getOrDefault(topic, 0);
            for (int p = 0; p < partitions; p++) {
                allPairs.add(TopicPartition.newBuilder()
                        .setTopic(topic)
                        .setPartition(p)
                        .build());
            }
        }
        if (sortedMemberIds.isEmpty()) {
            return out;
        }
        int cursor = 0;
        for (var pair : allPairs) {
            // Find next eligible member starting from cursor; if no
            // member subscribes to this topic, skip the pair entirely
            // (matches Kafka's RoundRobinAssignor semantics).
            for (int i = 0; i < sortedMemberIds.size(); i++) {
                int idx = (cursor + i) % sortedMemberIds.size();
                var member = sortedMemberIds.get(idx);
                if (subscriptionsByMember.getOrDefault(member, Set.of()).contains(pair.getTopic())) {
                    out.get(member).add(pair);
                    cursor = (idx + 1) % sortedMemberIds.size();
                    break;
                }
            }
        }
        var stable = new HashMap<String, List<TopicPartition>>(out.size());
        for (var entry : out.entrySet()) {
            var sorted = new ArrayList<>(entry.getValue());
            sorted.sort(Comparator.comparing(TopicPartition::getTopic).thenComparingInt(TopicPartition::getPartition));
            stable.put(entry.getKey(), List.copyOf(sorted));
        }
        return stable;
    }
}
