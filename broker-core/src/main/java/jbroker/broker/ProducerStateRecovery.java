package jbroker.broker;

import java.io.IOException;
import jbroker.storage.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rebuilds {@link ProducerStateManager} dedup state from a partition's
 * on-disk log — the record-batch v2 header carries {@code producer_id},
 * {@code producer_epoch} and {@code base_sequence} (the spec) precisely so
 * this state is derivable from the log.
 *
 * <p>Why: the dedup map is in-memory only. A SIGKILLed broker restarts
 * with a full log but an empty map, and {@code cached == null} used to
 * mean "trust and append" — so once the preferred-leader balancer
 * re-promoted the restarted broker, a producer retry of an
 * already-committed (pid, epoch, sequence) was appended a second time.
 * Found as duplicate payloads at consecutive offsets by
 * {@code scenario-chaos-with-load.sh}; violates the spec invariant #4.
 *
 * <p>Invoked lazily by {@link ProduceHandler} on the first idempotent
 * produce a partition sees after process start (mirrors the
 * {@code OffsetCacheRecovery} pattern used for {@code __consumer_offsets}).
 *
 * <p>Compaction note: {@code observeAppend} is monotonic in
 * {@code baseSequence}, so re-observing surviving batches of a compacted
 * log converges on the highest sequence per producer; batches compacted
 * away entirely can leave ancient producers unknown, which at worst
 * rejects a years-late retry with OUT_OF_ORDER_SEQUENCE — never a
 * double-append.
 */
public final class ProducerStateRecovery {

    private static final Logger log = LoggerFactory.getLogger(ProducerStateRecovery.class);
    private static final int READ_CHUNK_BYTES = 1 << 20;

    private ProducerStateRecovery() {}

    /** Scans the partition log and observes every idempotent batch. Returns batches observed. */
    public static int rebuild(LogManager logManager, String topic, int partition, ProducerStateManager producerState)
            throws IOException {
        var partitionLog = logManager.logFor(topic, partition);
        long end = partitionLog.nextOffset();
        long offset = 0;
        int observed = 0;
        while (offset < end) {
            var batches = partitionLog.read(offset, READ_CHUNK_BYTES);
            if (batches.isEmpty()) break; // truncated/compacted gap we cannot advance past
            for (var b : batches) {
                if (b.producerId() > 0) {
                    producerState.observeAppend(
                            new ProducerStateManager.DedupKey(topic, partition, b.producerId(), b.producerEpoch()),
                            b.baseSequence(),
                            b.records().size(),
                            b.baseOffset(),
                            b.lastOffset());
                    observed++;
                }
                offset = b.lastOffset() + 1;
            }
        }
        if (observed > 0) {
            log.info("rebuilt producer state for {}-{}: {} idempotent batches observed", topic, partition, observed);
        }
        return observed;
    }
}
