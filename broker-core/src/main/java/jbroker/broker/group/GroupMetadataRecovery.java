package jbroker.broker.group;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.storage.LogManager;
import jbroker.storage.RecordBatch;

/**
 * P7.8 — companion to {@link OffsetCacheRecovery}. Reads a
 * {@code __consumer_offsets} partition end-to-end and applies every
 * Type-2 (group-metadata) record it finds into a {@link GroupCoordinator}
 * via {@link GroupCoordinator#restoreGroup}. Latest-record-per-key wins
 * naturally from offset ordering.
 *
 * <p>Type-1 (offset-commit) records are silently skipped — they're handled
 * by {@link OffsetCacheRecovery} on a parallel walk. Both walks can run
 * over the same partition log in either order without conflict.
 */
public final class GroupMetadataRecovery {

    private GroupMetadataRecovery() {}

    /**
     * Walk every batch in the {@code __consumer_offsets} partition into
     * {@code coord}. {@code nowNs} is used to set the recovered members'
     * {@code lastHeartbeatNs} so they aren't immediately evicted by the
     * next eviction tick. Returns the number of group-metadata records
     * applied.
     */
    public static int rebuild(LogManager logManager, int partition, GroupCoordinator coord, long nowNs)
            throws IOException {
        return rebuild(logManager, ConsumerOffsetsTopic.NAME, partition, coord, nowNs);
    }

    /** Test-friendly form that accepts an explicit topic name. */
    public static int rebuild(LogManager logManager, String topic, int partition, GroupCoordinator coord, long nowNs)
            throws IOException {
        var log = logManager.logFor(topic, partition);
        long leo = log.nextOffset();
        if (leo == 0) return 0;

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
                    var groupOpt = ConsumerOffsetsTopic.decodeGroupMetadataKey(rec.key());
                    if (groupOpt.isEmpty()) continue; // not a group-metadata record (offset commit, etc.)
                    var snapshot = ConsumerOffsetsTopic.decodeGroupMetadataValue(rec.value());
                    coord.restoreGroup(groupOpt.get(), snapshot, nowNs);
                    applied++;
                }
            } catch (IllegalArgumentException e) {
                // Truncated trailing batch — stop here.
                buf.position(mark);
                break;
            }
        }
        return applied;
    }
}
