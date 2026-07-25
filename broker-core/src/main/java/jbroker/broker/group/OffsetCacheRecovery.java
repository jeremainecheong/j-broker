package jbroker.broker.group;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.proto.common.TopicPartition;
import jbroker.storage.ControlRecord;
import jbroker.storage.LogManager;
import jbroker.storage.RecordBatch;

/**
 * Reads a {@code __consumer_offsets} partition's log end-to-end, decoding
 * every record and applying the latest one per
 * {@code (group, topic, partition)} key into an {@link OffsetCache}.
 *
 * <p>Compaction is read-time: real on-disk compaction is not yet implemented.
 * Until then, the recovery walk simply iterates the entire log and the
 * "last write wins" semantics fall out of natural offset ordering.
 *
 * <p>Transactional offset commits replay through the same walk:
 * a TRANSACTIONAL batch (staged {@code TxnOffsetCommit} records under the
 * producer's id/epoch) stages into the supplied {@link TxnOffsetStaging}
 * instead of the cache, and a control batch decides the producer's staged
 * offsets — COMMIT folds them into the cache, ABORT discards them. The
 * fold is deterministic from the log alone (staged batch + marker), so
 * replay never re-appends; the regular offset records the live fold path
 * appends after a commit simply re-apply idempotently when the walk
 * reaches them. Undecided staged offsets survive the walk inside the
 * staging — pass the coordinator's live instance
 * ({@link GroupCoordinator#txnOffsetStaging()}) so a later marker still
 * finds them; the staging-less overloads reconstruct the committed view
 * only.
 *
 * <p>Group-metadata (Type-2) records are skipped here. They're written
 * by the coordinator failover work and read by a separate recovery walk
 * that rebuilds {@link GroupCoordinator} state.
 */
public final class OffsetCacheRecovery {

    private OffsetCacheRecovery() {}

    /**
     * Walk every batch in the {@code (topic, partition)} log into
     * {@code cache}. Returns the number of offset-commit records applied
     * (directly or by a commit-marker fold). Group-metadata records are
     * tallied separately and not applied.
     */
    public static int rebuild(LogManager logManager, int partition, OffsetCache cache) throws IOException {
        return rebuild(logManager, ConsumerOffsetsTopic.NAME, partition, cache);
    }

    /** Test-friendly form that accepts an explicit topic name. */
    public static int rebuild(LogManager logManager, String topic, int partition, OffsetCache cache)
            throws IOException {
        return rebuild(logManager, topic, partition, cache, new TxnOffsetStaging());
    }

    /**
     * Full form: transactional batches stage into {@code staging} and
     * control batches decide them, so staged-but-undecided transactions
     * are reconstructed exactly where the walk stops.
     */
    public static int rebuild(
            LogManager logManager, String topic, int partition, OffsetCache cache, TxnOffsetStaging staging)
            throws IOException {
        var log = logManager.logFor(topic, partition);
        long leo = log.nextOffset();
        if (leo == 0) return 0;

        // Stream the entire partition into a buffer in chunks, then decode
        // batch-by-batch. Single-coordinator-partition recoveries are bounded
        // by the partition's data volume; for typical commit rates (one
        // commit per consumer poll cycle) a partition holds 100s of MB at
        // most. Chunked transfer keeps peak memory bounded to a single
        // segment's worth of records.
        var sink = new ByteArrayOutputStream();
        long offset = 0;
        while (offset < leo) {
            int read = (int) log.transferTo(offset, /*maxBytes*/ 1 << 20, sink);
            if (read == 0) break;
            offset += read;
        }
        var buf = ByteBuffer.wrap(sink.toByteArray());
        int applied = 0;
        while (buf.remaining() >= RecordBatch.BATCH_OVERHEAD) {
            int mark = buf.position();
            try {
                var parsed = RecordBatch.decode(buf);
                if (parsed.control() && parsed.producerId() >= 0) {
                    boolean commit = parsed.controlRecord().type() == ControlRecord.Type.COMMIT;
                    for (var folded :
                            staging.onMarker(partition, parsed.producerId(), parsed.producerEpoch(), commit, cache)) {
                        applied += folded.offsets().size();
                    }
                    continue;
                }
                boolean transactional = parsed.transactional() && parsed.producerId() >= 0;
                Map<String, Map<TopicPartition, OffsetCache.OffsetAndMetadata>> stagedByGroup =
                        transactional ? new LinkedHashMap<>() : null;
                for (var rec : parsed.records()) {
                    var keyOpt = ConsumerOffsetsTopic.decodeOffsetKey(rec.key());
                    if (keyOpt.isEmpty()) continue; // not an offset record (type-2 group metadata, etc.)
                    var key = keyOpt.get();
                    if (rec.value() == null) {
                        // Tombstone from offset expiry — the commit is gone,
                        // and must stay gone across restarts. Never written
                        // transactionally; a transactional tombstone is
                        // skipped rather than staged (nothing to fold).
                        if (!transactional) cache.remove(key.group(), key.topic(), key.partition());
                        continue;
                    }
                    var value = ConsumerOffsetsTopic.decodeOffsetValue(rec.value());
                    var oam = new OffsetCache.OffsetAndMetadata(
                            value.offset(), value.leaderEpoch(), value.metadata(), value.commitTimestamp());
                    if (transactional) {
                        stagedByGroup
                                .computeIfAbsent(key.group(), g -> new LinkedHashMap<>())
                                .put(
                                        TopicPartition.newBuilder()
                                                .setTopic(key.topic())
                                                .setPartition(key.partition())
                                                .build(),
                                        oam);
                    } else {
                        cache.put(key.group(), key.topic(), key.partition(), oam);
                        applied++;
                    }
                }
                if (transactional) {
                    for (var e : stagedByGroup.entrySet()) {
                        staging.stage(e.getKey(), partition, parsed.producerId(), parsed.producerEpoch(), e.getValue());
                    }
                }
            } catch (IllegalArgumentException e) {
                // Truncated trailing batch (mid-write crash) — stop here.
                buf.position(mark);
                break;
            }
        }
        return applied;
    }
}
