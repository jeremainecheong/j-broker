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
 * Kafka's classic range assignor: per topic, sort members that subscribe to
 * the topic by id, sort that topic's partitions ascending, then split into
 * contiguous runs across members. Earlier members may receive one extra
 * partition when the count doesn't divide evenly.
 *
 * <p>Multi-topic groups: each topic is partitioned independently, then the
 * per-topic results are concatenated per member. Members that don't
 * subscribe to a particular topic skip it.
 */
public final class RangeAssignor implements PartitionAssignor {

    public static final String NAME = "range";

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
        // Walk topics in stable order so the output is reproducible.
        var sortedTopics = new ArrayList<>(partitionsByTopic.keySet());
        sortedTopics.sort(Comparator.naturalOrder());
        for (var topic : sortedTopics) {
            var subscribers = new ArrayList<String>();
            for (var m : sortedMemberIds) {
                if (subscriptionsByMember.getOrDefault(m, Set.of()).contains(topic)) {
                    subscribers.add(m);
                }
            }
            if (subscribers.isEmpty()) continue;
            int partitions = partitionsByTopic.getOrDefault(topic, 0);
            int base = partitions / subscribers.size();
            int extras = partitions % subscribers.size();
            int cursor = 0;
            for (int i = 0; i < subscribers.size(); i++) {
                int chunk = base + (i < extras ? 1 : 0);
                var member = subscribers.get(i);
                for (int p = 0; p < chunk; p++) {
                    out.get(member)
                            .add(TopicPartition.newBuilder()
                                    .setTopic(topic)
                                    .setPartition(cursor + p)
                                    .build());
                }
                cursor += chunk;
            }
        }
        // Sort each member's assignment by (topic, partition) so equality
        // checks in tests are stable.
        var stable = new HashMap<String, List<TopicPartition>>(out.size());
        for (var entry : out.entrySet()) {
            var sorted = new ArrayList<>(entry.getValue());
            sorted.sort(Comparator.comparing(TopicPartition::getTopic).thenComparingInt(TopicPartition::getPartition));
            stable.put(entry.getKey(), List.copyOf(sorted));
        }
        return stable;
    }
}
