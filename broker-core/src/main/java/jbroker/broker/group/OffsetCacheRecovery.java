package jbroker.broker.group;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import jbroker.broker.ConsumerOffsetsTopic;
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
 * <p>Group-metadata (Type-2) records are skipped here. They're written
 * by the coordinator failover work and read by a separate recovery walk
 * that rebuilds {@link GroupCoordinator} state.
 */
public final class OffsetCacheRecovery {

    private OffsetCacheRecovery() {}

    /**
     * Walk every batch in the {@code (topic, partition)} log into
     * {@code cache}. Returns the number of offset-commit records applied.
     * Group-metadata records are tallied separately and not applied.
     */
    public static int rebuild(LogManager logManager, int partition, OffsetCache cache) throws IOException {
        return rebuild(logManager, ConsumerOffsetsTopic.NAME, partition, cache);
    }

    /** Test-friendly form that accepts an explicit topic name. */
    public static int rebuild(LogManager logManager, String topic, int partition, OffsetCache cache)
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
                for (var rec : parsed.records()) {
                    var keyOpt = ConsumerOffsetsTopic.decodeOffsetKey(rec.key());
                    if (keyOpt.isEmpty()) continue; // not an offset record (type-2 group metadata, etc.)
                    var value = ConsumerOffsetsTopic.decodeOffsetValue(rec.value());
                    cache.put(
                            keyOpt.get().group(),
                            keyOpt.get().topic(),
                            keyOpt.get().partition(),
                            new OffsetCache.OffsetAndMetadata(
                                    value.offset(), value.leaderEpoch(), value.metadata(), value.commitTimestamp()));
                    applied++;
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
