package jbroker.broker.group;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import jbroker.proto.common.TopicPartition;

/**
 * Staged transactional offset commits for the groups this broker
 * coordinates. A {@code TxnOffsetCommit} stages its offsets here keyed by
 * {@code (group, producerId)} — the committed view ({@link OffsetCache})
 * is untouched until the transaction's control batch reaches the group's
 * {@code __consumer_offsets} partition: a COMMIT marker folds the staged
 * offsets into the cache (and hands them back so the caller can append
 * them as regular offset records for durability), an ABORT marker
 * discards them.
 *
 * <p>Durability of the staged state itself comes from the log, not this
 * map: the RPC path appends the staged offsets to the group's coordinator
 * partition as a TRANSACTIONAL batch under {@code (producerId, epoch)}
 * before staging, and {@link OffsetCacheRecovery} replays transactional
 * batches back through {@link #stage} and control batches through
 * {@link #onMarker} — so a coordinator restart reconstructs
 * staged-but-undecided transactions exactly.
 *
 * <p>Epoch fencing: a stage at a lower producer epoch than what is
 * already staged — or than the last marker-decided epoch — for the same
 * {@code (group, producerId)} is refused with
 * {@link StageOutcome#PRODUCER_FENCED}. A stage at a HIGHER epoch
 * replaces the previous epoch's staged offsets (that transaction was
 * abandoned when the producer re-registered; its late marker, carrying
 * the lower epoch, no longer matches and leaves the new stage alone).
 * A marker decides only staged entries at or below its epoch.
 *
 * <p>Scoping: entries carry the coordinator partition they were staged
 * (and appended) under, and a marker observed on partition {@code p}
 * decides only entries staged under {@code p} — replay of one partition
 * must never decide offsets whose durable record lives in another.
 *
 * <p>Thread-safety: all methods synchronize on this instance.
 */
public final class TxnOffsetStaging {

    public enum StageOutcome {
        OK,
        PRODUCER_FENCED
    }

    /** One group's folded (committed) offsets, returned by {@link #onMarker} on COMMIT. */
    public record FoldedOffsets(String groupId, Map<TopicPartition, OffsetCache.OffsetAndMetadata> offsets) {
        public FoldedOffsets {
            offsets = Map.copyOf(offsets);
        }
    }

    private record Key(String group, long producerId) {}

    private static final class Staged {
        final int coordinatorPartition;
        int producerEpoch;
        final Map<TopicPartition, OffsetCache.OffsetAndMetadata> offsets = new LinkedHashMap<>();

        Staged(int coordinatorPartition, int producerEpoch) {
            this.coordinatorPartition = coordinatorPartition;
            this.producerEpoch = producerEpoch;
        }
    }

    private final Map<Key, Staged> staged = new HashMap<>();

    /**
     * Producer epoch of the last marker that decided each key. A zombie
     * {@code TxnOffsetCommit} arriving with a lower epoch AFTER its
     * transaction was decided must not open a fresh stage that no marker
     * will ever close. An EQUAL epoch restage is legal — the producer's
     * next transaction reuses the epoch, and log order attributes it to
     * the next marker.
     */
    private final Map<Key, Integer> lastDecidedEpoch = new HashMap<>();

    /**
     * Stage {@code offsets} for {@code (groupId, producerId)} without
     * touching the committed view. Same-epoch stages merge per partition
     * (later wins); a higher epoch replaces the doomed lower-epoch stage.
     */
    public synchronized StageOutcome stage(
            String groupId,
            int coordinatorPartition,
            long producerId,
            int producerEpoch,
            Map<TopicPartition, OffsetCache.OffsetAndMetadata> offsets) {
        var key = new Key(groupId, producerId);
        Integer decided = lastDecidedEpoch.get(key);
        if (decided != null && producerEpoch < decided) return StageOutcome.PRODUCER_FENCED;
        var existing = staged.get(key);
        if (existing != null && producerEpoch < existing.producerEpoch) return StageOutcome.PRODUCER_FENCED;
        if (offsets.isEmpty()) return StageOutcome.OK;
        if (existing == null || producerEpoch > existing.producerEpoch) {
            existing = new Staged(coordinatorPartition, producerEpoch);
            staged.put(key, existing);
        }
        existing.offsets.putAll(offsets);
        return StageOutcome.OK;
    }

    /**
     * A control batch for {@code producerId} was observed on
     * {@code coordinatorPartition}. Decides every entry staged under that
     * partition for the producer at or below {@code producerEpoch}:
     * COMMIT folds into {@code cache} and returns the folded offsets per
     * group (the caller appends them as regular offset records for
     * durability — replay does NOT re-append, the on-disk staged batch
     * plus marker already reproduce the fold); ABORT discards and returns
     * an empty list. A marker with no matching stage is a no-op, which
     * makes redelivered markers idempotent.
     */
    public synchronized List<FoldedOffsets> onMarker(
            int coordinatorPartition, long producerId, int producerEpoch, boolean commit, OffsetCache cache) {
        var folded = new ArrayList<FoldedOffsets>();
        var it = staged.entrySet().iterator();
        while (it.hasNext()) {
            var e = it.next();
            if (e.getKey().producerId() != producerId) continue;
            var entry = e.getValue();
            if (entry.coordinatorPartition != coordinatorPartition) continue;
            if (entry.producerEpoch > producerEpoch) continue; // stale marker of a replaced stage
            it.remove();
            lastDecidedEpoch.merge(e.getKey(), producerEpoch, Math::max);
            if (!commit) continue;
            for (var offset : entry.offsets.entrySet()) {
                cache.put(e.getKey().group(), offset.getKey(), offset.getValue());
            }
            folded.add(new FoldedOffsets(e.getKey().group(), entry.offsets));
        }
        return List.copyOf(folded);
    }

    /** Staged offsets for {@code (groupId, producerId)}; empty when none. Test/inspection accessor. */
    public synchronized Map<TopicPartition, OffsetCache.OffsetAndMetadata> stagedFor(String groupId, long producerId) {
        var entry = staged.get(new Key(groupId, producerId));
        return entry == null ? Map.of() : Map.copyOf(entry.offsets);
    }

    /** Number of {@code (group, producerId)} pairs with a staged, undecided transaction. */
    public synchronized int stagedCount() {
        return staged.size();
    }
}
