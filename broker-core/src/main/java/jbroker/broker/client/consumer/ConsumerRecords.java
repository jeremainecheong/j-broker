package jbroker.broker.client.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jbroker.proto.common.TopicPartition;

/**
 * Batch returned by {@link Consumer#poll}. Provides per-tp accessors and
 * a convenient flat {@link #all} iterator.
 */
public final class ConsumerRecords<K, V> implements Iterable<ConsumerRecord<K, V>> {

    private final Map<TopicPartition, List<ConsumerRecord<K, V>>> records;

    public ConsumerRecords(Map<TopicPartition, List<ConsumerRecord<K, V>>> records) {
        var copy = new HashMap<TopicPartition, List<ConsumerRecord<K, V>>>(records.size());
        for (var e : records.entrySet()) {
            copy.put(e.getKey(), List.copyOf(e.getValue()));
        }
        this.records = Collections.unmodifiableMap(copy);
    }

    public static <K, V> ConsumerRecords<K, V> empty() {
        return new ConsumerRecords<>(Map.of());
    }

    public List<ConsumerRecord<K, V>> records(TopicPartition tp) {
        return records.getOrDefault(tp, List.of());
    }

    public int count() {
        int total = 0;
        for (var l : records.values()) total += l.size();
        return total;
    }

    public boolean isEmpty() {
        return records.values().stream().allMatch(List::isEmpty);
    }

    @Override
    public java.util.Iterator<ConsumerRecord<K, V>> iterator() {
        return all().iterator();
    }

    public List<ConsumerRecord<K, V>> all() {
        var out = new ArrayList<ConsumerRecord<K, V>>(count());
        for (var l : records.values()) out.addAll(l);
        return List.copyOf(out);
    }
}
