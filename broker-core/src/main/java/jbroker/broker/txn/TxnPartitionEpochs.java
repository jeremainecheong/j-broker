package jbroker.broker.txn;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-partition high-water producer epochs, the partition-local half of
 * transactional fencing. Every observed batch of a producer (data or
 * control) raises that producer's floor on that partition; a transactional
 * produce carrying a lower epoch is a zombie — its coordinator already
 * bumped the epoch (InitTransactions re-registration or timeout abort)
 * and delivered markers carrying the bump — and is refused with
 * {@code PRODUCER_FENCED}.
 *
 * <p>Fed from three places so the view survives role changes:
 * <ul>
 *   <li>the leader's produce path (accepted transactional appends),</li>
 *   <li>the leader's marker append path (control batches carry the bumped
 *       epoch that fences the previous one),</li>
 *   <li>the follower's replica apply + the lazy log recovery walk, so an
 *       ex-follower taking over leadership fences with the same floor the
 *       old leader had.</li>
 * </ul>
 *
 * <p>In-memory only, monotone per key, safe to rebuild from the log at any
 * time. Bounded by live producers per partition; evicted with the topic.
 */
public final class TxnPartitionEpochs {

    private record Tp(String topic, int partition) {}

    private final Map<Tp, ConcurrentHashMap<Long, Integer>> byPartition = new ConcurrentHashMap<>();

    /** Raise {@code producerId}'s epoch floor on the partition. Lower observations are no-ops. */
    public void observe(String topic, int partition, long producerId, int producerEpoch) {
        byPartition
                .computeIfAbsent(new Tp(topic, partition), k -> new ConcurrentHashMap<>())
                .merge(producerId, producerEpoch, Math::max);
    }

    /** Highest epoch observed for {@code producerId} on the partition, or -1 if never seen. */
    public int maxEpochOf(String topic, int partition, long producerId) {
        var epochs = byPartition.get(new Tp(topic, partition));
        if (epochs == null) return -1;
        return epochs.getOrDefault(producerId, -1);
    }

    /** Drop every entry of {@code topic} — the topic-deletion path. */
    public void evictTopic(String topic) {
        byPartition.keySet().removeIf(tp -> tp.topic().equals(topic));
    }
}
