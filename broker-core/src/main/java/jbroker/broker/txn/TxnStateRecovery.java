package jbroker.broker.txn;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import jbroker.storage.LogManager;
import jbroker.storage.RecordBatch;

/**
 * Companion to {@link jbroker.broker.group.GroupMetadataRecovery}, for
 * transactions. Reads a {@code __transaction_state} partition end-to-end
 * and applies every transaction-state record it finds into a
 * {@link TxnCoordinator} via {@link TxnCoordinator#restore}.
 * Latest-record-per-key wins naturally from offset ordering, matching
 * what compaction preserves.
 *
 * <p>Run on coordinator activation (this broker becomes leader of the
 * partition). After the walk, the caller drains
 * {@link TxnCoordinator#pendingMarkers()} to resume any marker delivery
 * the previous coordinator did not finish.
 */
public final class TxnStateRecovery {

    private TxnStateRecovery() {}

    /**
     * Walk every batch in the {@code __transaction_state} partition into
     * {@code coord}. Returns the number of transaction-state records
     * applied (overwrites included).
     */
    public static int rebuild(LogManager logManager, int partition, TxnCoordinator coord) throws IOException {
        return rebuild(logManager, TxnStateTopic.NAME, partition, coord);
    }

    /** Test-friendly form that accepts an explicit topic name. */
    public static int rebuild(LogManager logManager, String topic, int partition, TxnCoordinator coord)
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
                    var txnIdOpt = TxnStateTopic.decodeTxnKey(rec.key());
                    if (txnIdOpt.isEmpty()) continue; // unknown record type — skip (forward compat)
                    coord.restore(TxnStateTopic.decodeTxnValue(txnIdOpt.get(), rec.value()));
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
