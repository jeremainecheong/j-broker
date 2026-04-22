package jbroker.broker.group;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import jbroker.proto.common.TopicPartition;

/**
 * In-memory view of the latest committed offset per
 * {@code (group, topic, partition)}. Persisted form lives in the
 * {@code __consumer_offsets} compacted internal topic — every successful
 * {@code CommitOffsets} writes a record there AND updates this cache.
 *
 * <p>On coordinator activation (broker becomes leader of a
 * {@code __consumer_offsets} partition), the recovery walk in
 * {@link OffsetCacheRecovery} replays the partition log and rebuilds this
 * cache so {@code FetchOffsets} can answer immediately.
 */
public final class OffsetCache {

    public record OffsetAndMetadata(long offset, int leaderEpoch, String metadata, long commitTimestamp) {
        public OffsetAndMetadata {
            if (metadata == null) metadata = "";
        }
    }

    private record Key(String group, String topic, int partition) {}

    private final ConcurrentHashMap<Key, OffsetAndMetadata> entries = new ConcurrentHashMap<>();

    public void put(String group, String topic, int partition, OffsetAndMetadata oam) {
        entries.put(new Key(group, topic, partition), oam);
    }

    public void put(String group, TopicPartition tp, OffsetAndMetadata oam) {
        put(group, tp.getTopic(), tp.getPartition(), oam);
    }

    public Optional<OffsetAndMetadata> get(String group, String topic, int partition) {
        return Optional.ofNullable(entries.get(new Key(group, topic, partition)));
    }

    public Optional<OffsetAndMetadata> get(String group, TopicPartition tp) {
        return get(group, tp.getTopic(), tp.getPartition());
    }

    /** Test/debug: total entry count. */
    public int size() {
        return entries.size();
    }

    /**
     * snapshot every committed offset for {@code group}. Used by the
     * admin REST layer to compute per-partition lag against the log HWM.
     * Iteration is weakly consistent per {@link ConcurrentHashMap} — a new
     * commit that lands mid-iteration may or may not appear.
     */
    public java.util.Map<TopicPartition, OffsetAndMetadata> snapshotForGroup(String group) {
        var out = new java.util.HashMap<TopicPartition, OffsetAndMetadata>();
        for (var e : entries.entrySet()) {
            if (!e.getKey().group().equals(group)) continue;
            out.put(
                    TopicPartition.newBuilder()
                            .setTopic(e.getKey().topic())
                            .setPartition(e.getKey().partition())
                            .build(),
                    e.getValue());
        }
        return java.util.Map.copyOf(out);
    }
}
