package jbroker.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * An ordered list of {@link LogSegment}s forming one topic-partition's log.
 * Appends always land on the active (last) segment; when it exceeds
 * {@link Config#segmentBytes}, a new segment rolls. Retention deletes closed
 * segments older than {@link Config#retentionMillis}.
 *
 * <p>Concurrency: appends are serialised on {@code this}; reads are
 * lock-free via the segments list ({@link CopyOnWriteArrayList}). A segment
 * being read during a concurrent {@link #retain} is safe — its files are
 * deleted only after the list swap, and open {@link java.nio.channels.FileChannel}
 * handles on Linux keep the underlying inode alive until closed.
 */
public final class Log implements AutoCloseable {

    public record Config(long segmentBytes, long retentionMillis, int indexIntervalBytes) {
        public static Config defaults() {
            return new Config(128L * 1024 * 1024, 7L * 24 * 60 * 60 * 1000, LogSegment.DEFAULT_INDEX_INTERVAL_BYTES);
        }
    }

    private final Path dir;
    private final Config config;

    /**
     * Roll threshold, initially {@link Config#segmentBytes}. Volatile — a
     * per-topic {@code segment.bytes} override can land after the log is
     * opened (config updates commit through the metadata log while the log
     * serves traffic), and the cleaner tick pushes the effective value here.
     */
    private volatile long segmentBytes;

    private final CopyOnWriteArrayList<LogSegment> segments = new CopyOnWriteArrayList<>();
    /**
     * Mutation lock. Replaces `synchronized (this)` on the hot
     * append / nextOffset paths so virtual threads running produce/fetch
     * no longer pin their carrier OS thread while holding a monitor.
     * Cold paths (retain, truncate, compact, close) use the same lock
     * for mutual exclusion; they're off the request path so pinning
     * would have been a non-issue anyway, but keeping one lock avoids
     * interleaved retention mid-append.
     */
    private final java.util.concurrent.locks.ReentrantLock lock = new java.util.concurrent.locks.ReentrantLock();

    /**
     * Per-log transaction bookkeeping: ongoing ranges for the LSO and the
     * aborted-txn index. Mutated on the append/truncate paths (under
     * {@link #lock}); read lock-free by fetch paths — the state object
     * synchronizes internally.
     */
    private final TransactionState txnState = new TransactionState();

    /**
     * Invoked before the first byte of every control batch hits disk. The
     * {@link LogManager} installs a gate that re-stamps the data
     * directory's {@link FormatVersion} marker so a binary too old to
     * understand control batches refuses the directory instead of decoding
     * markers as data. Default no-op: a log opened without a manager has no
     * data-dir marker to guard.
     */
    @FunctionalInterface
    public interface ControlAppendGate {
        void ensureControlWritable() throws IOException;
    }

    private volatile ControlAppendGate controlAppendGate = () -> {};

    public void setControlAppendGate(ControlAppendGate gate) {
        this.controlAppendGate = java.util.Objects.requireNonNull(gate);
    }

    /**
     * Invoked after every successful append ({@link #append},
     * {@link #appendRaw}, {@link #appendControl}), outside {@link #lock} so
     * a slow listener can never extend the append critical section.
     * Opaque to storage — the replication layer parks its data-availability
     * waits on it. Set post-construction; null keeps the fast path free.
     */
    private volatile Runnable appendListener;

    public void setAppendListener(Runnable listener) {
        this.appendListener = listener;
    }

    private void notifyAppend() {
        var listener = appendListener;
        if (listener != null) listener.run();
    }

    private Log(Path dir, Config config) {
        this.dir = dir;
        this.config = config;
        this.segmentBytes = config.segmentBytes();
    }

    /** Update the roll threshold on a live log. Non-positive values are ignored. */
    public void reconfigureSegmentBytes(long newSegmentBytes) {
        if (newSegmentBytes > 0) this.segmentBytes = newSegmentBytes;
    }

    // ---- Flush policy ----
    //
    // Default (-1/-1): fsync happens on segment roll and on explicit
    // force(); durability between fsyncs comes from replication, not the
    // local disk. The optional knobs bound the unflushed window for
    // single-copy-sensitive deployments: flushMessages forces after N
    // appended records, flushMillis is enforced by LogManager's flush tick
    // calling flushIfDue.

    private volatile long flushMessages = -1;
    private volatile long flushMillis = -1;
    /** Records appended to the active segment since its last fsync. Guarded by {@link #lock}. */
    private long recordsSinceForce;
    /** Wall-clock of the first unflushed append; -1 when everything is flushed. */
    private volatile long firstUnforcedMillis = -1;
    /** Times the flush policy forced the active segment. Observability + tests. */
    private final java.util.concurrent.atomic.AtomicLong policyFlushCount =
            new java.util.concurrent.atomic.AtomicLong();

    /** Update the flush policy on a live log. Non-positive values disable the respective trigger. */
    public void reconfigureFlushPolicy(long newFlushMessages, long newFlushMillis) {
        this.flushMessages = newFlushMessages;
        this.flushMillis = newFlushMillis;
    }

    /** Times the flush policy (count or age trigger) forced the active segment. */
    public long policyFlushCount() {
        return policyFlushCount.get();
    }

    /**
     * Force the active segment if the age trigger is due. Called by
     * LogManager's flush tick; cheap no-op when the policy is off or
     * nothing is unflushed.
     */
    public void flushIfDue(long nowMillis) throws IOException {
        long millis = flushMillis;
        long first = firstUnforcedMillis;
        if (millis <= 0 || first < 0 || nowMillis - first < millis) return;
        lock.lock();
        try {
            if (firstUnforcedMillis < 0) return; // raced a roll/force
            forceActiveLocked();
        } finally {
            lock.unlock();
        }
    }

    /** Track an append and apply the count trigger. Callers hold {@link #lock}. */
    private void onAppendedLocked(int recordCount, long nowMillis) throws IOException {
        recordsSinceForce += recordCount;
        if (firstUnforcedMillis < 0) firstUnforcedMillis = nowMillis;
        long messages = flushMessages;
        if (messages > 0 && recordsSinceForce >= messages) {
            forceActiveLocked();
        }
    }

    private void forceActiveLocked() throws IOException {
        segments.get(segments.size() - 1).force();
        recordsSinceForce = 0;
        firstUnforcedMillis = -1;
        policyFlushCount.incrementAndGet();
    }

    /** A roll or full force fsynced everything appended so far. Callers hold {@link #lock}. */
    private void markFlushedLocked() {
        recordsSinceForce = 0;
        firstUnforcedMillis = -1;
    }

    public static Log open(Path dir, Config config) throws IOException {
        Files.createDirectories(dir);
        var log = new Log(dir, config);
        log.loadSegments();
        if (log.segments.isEmpty()) {
            log.segments.add(LogSegment.open(dir, 0L, config.indexIntervalBytes()));
        }
        // Rebuild LSO + aborted-txn state from the recovery scan each
        // segment already ran (the same decode pass that restored
        // nextOffset); replay in segment order = log order.
        for (var seg : log.segments) {
            for (var event : seg.drainRecoveredTxnEvents()) {
                log.txnState.apply(event);
            }
        }
        return log;
    }

    private void loadSegments() throws IOException {
        var seen = new ArrayList<Long>();
        try (var stream = Files.list(dir)) {
            stream.filter(p -> p.getFileName().toString().endsWith(".log"))
                    .map(p -> p.getFileName().toString().replace(".log", ""))
                    .forEach(s -> {
                        try {
                            seen.add(Long.parseLong(s));
                        } catch (NumberFormatException ignored) {
                            /* non-conforming filename — skip */
                        }
                    });
        }
        seen.sort(Comparator.naturalOrder());
        for (var base : seen) {
            segments.add(LogSegment.open(dir, base, config.indexIntervalBytes()));
        }
    }

    /**
     * Append a pre-encoded batch to the end of the log at
     * {@code expectedBaseOffset} (≥ {@link #nextOffset()} at call time; a
     * forward gap means the leader retained/compacted away the range in
     * between and the follower adopts the leader's earliest available
     * batch). Used by the follower replication path to preserve every byte
     * of the leader's batch header.
     */
    public long appendRaw(byte[] encodedBatch, long expectedBaseOffset) throws IOException {
        long assignedLast;
        lock.lock();
        try {
            // Peek the plaintext attributes so replicated transactional and
            // control batches keep this replica's LSO and aborted index in
            // step with the leader's — byte-identical replication carries
            // the flags for free.
            short attributes = encodedBatch.length >= RecordBatch.BATCH_OVERHEAD
                    ? java.nio.ByteBuffer.wrap(encodedBatch).getShort(RecordBatch.ATTRIBUTES_OFFSET)
                    : 0;
            boolean control = RecordBatch.isControl(attributes);
            if (control) {
                // Stamp the format marker BEFORE the marker bytes land: a
                // crash in between over-claims (marker bumped, no control
                // batch yet), never under-claims.
                controlAppendGate.ensureControlWritable();
            }
            var active = segments.get(segments.size() - 1);
            active.appendRaw(encodedBatch, expectedBaseOffset);
            assignedLast = active.nextOffset() - 1;
            if (control) {
                var parsed = RecordBatch.decode(java.nio.ByteBuffer.wrap(encodedBatch));
                if (parsed.producerId() >= 0) {
                    txnState.onControl(
                            parsed.producerId(), parsed.controlRecord().type());
                }
            } else if (RecordBatch.isTransactional(attributes)) {
                long producerId = java.nio.ByteBuffer.wrap(encodedBatch).getLong(RecordBatch.PRODUCER_ID_OFFSET);
                if (producerId >= 0) {
                    txnState.onTransactionalData(producerId, expectedBaseOffset, assignedLast);
                }
            }
            if (active.sizeBytes() >= segmentBytes) {
                var next = LogSegment.open(dir, active.nextOffset(), config.indexIntervalBytes());
                active.force();
                segments.add(next);
                markFlushedLocked();
            } else {
                onAppendedLocked((int) (active.nextOffset() - expectedBaseOffset), System.currentTimeMillis());
            }
        } finally {
            lock.unlock();
        }
        notifyAppend();
        return assignedLast;
    }

    public long append(List<Record> records, long nowMillis) throws IOException {
        return append(records, nowMillis, /*producerId*/ -1L, /*producerEpoch*/ (short) -1, /*baseSequence*/ -1);
    }

    /**
     * Audit-finding #1 — append that preserves the caller-supplied idempotent-
     * producer fields so follower replication can observe dedup state.
     * Batches carry {@code partitionLeaderEpoch = 0} (pre-lineage callers).
     */
    public long append(List<Record> records, long nowMillis, long producerId, short producerEpoch, int baseSequence)
            throws IOException {
        return append(records, nowMillis, producerId, producerEpoch, baseSequence, /*partitionLeaderEpoch*/ 0);
    }

    /**
     * Full append — additionally stamps the current {@code
     * partitionLeaderEpoch} into the batch header. The epoch is the log's
     * LINEAGE marker: followers replicate the batch byte-identically and
     * derive their leader-epoch checkpoints from these headers, which is
     * what makes divergent tails detectable on fetch.
     */
    public long append(
            List<Record> records,
            long nowMillis,
            long producerId,
            short producerEpoch,
            int baseSequence,
            int partitionLeaderEpoch)
            throws IOException {
        return append(
                records, nowMillis, producerId, producerEpoch, baseSequence, partitionLeaderEpoch, Compression.NONE);
    }

    /**
     * Full append with a records-section codec. The produce path threads
     * the codec of the client's batch through here so a compressed produce
     * stays compressed on disk across the decode/re-encode hop that stamps
     * broker-owned header fields (baseOffset, leader epoch, timestamps).
     */
    public long append(
            List<Record> records,
            long nowMillis,
            long producerId,
            short producerEpoch,
            int baseSequence,
            int partitionLeaderEpoch,
            Compression codec)
            throws IOException {
        return append(
                records,
                nowMillis,
                producerId,
                producerEpoch,
                baseSequence,
                partitionLeaderEpoch,
                codec,
                /*transactional*/ false);
    }

    /**
     * Full append with the transactional flag: the batch is stamped with
     * the transactional attribute bit and opens (or extends)
     * {@code producerId}'s ongoing transaction, holding the
     * {@linkplain #lastStableOffset last stable offset} at the
     * transaction's first offset until a control batch decides it.
     */
    public long append(
            List<Record> records,
            long nowMillis,
            long producerId,
            short producerEpoch,
            int baseSequence,
            int partitionLeaderEpoch,
            Compression codec,
            boolean transactional)
            throws IOException {
        long assignedLast;
        lock.lock();
        try {
            var active = segments.get(segments.size() - 1);
            long baseOffset = active.nextOffset();
            long firstTimestamp = nowMillis;
            long maxTimestamp = nowMillis;
            active.append(
                    firstTimestamp,
                    maxTimestamp,
                    records,
                    producerId,
                    producerEpoch,
                    baseSequence,
                    partitionLeaderEpoch,
                    codec,
                    transactional);
            assignedLast = active.nextOffset() - 1;
            if (transactional) {
                txnState.onTransactionalData(producerId, baseOffset, assignedLast);
            }
            if (active.sizeBytes() >= segmentBytes) {
                var next = LogSegment.open(dir, active.nextOffset(), config.indexIntervalBytes());
                active.force();
                segments.add(next);
                markFlushedLocked();
            } else {
                // Wall clock, not the caller's record timestamp — the age
                // trigger bounds real elapsed time since the append.
                onAppendedLocked(records.size(), System.currentTimeMillis());
            }
        } finally {
            lock.unlock();
        }
        notifyAppend();
        return assignedLast;
    }

    /**
     * Append a control batch deciding {@code producerId}'s ongoing
     * transaction: COMMIT releases it, ABORT additionally records the data
     * range in the aborted-txn index. Consumes one offset; never
     * compressed. Returns the marker's assigned offset.
     *
     * <p>The first control batch a data directory ever sees re-stamps its
     * format marker via the {@link ControlAppendGate} before the bytes
     * land, so a rolling downgrade to a pre-control binary refuses loudly.
     */
    public long appendControl(
            long producerId, short producerEpoch, ControlRecord marker, long nowMillis, int partitionLeaderEpoch)
            throws IOException {
        if (producerId < 0) {
            throw new IllegalArgumentException("control batch requires producerId >= 0, got " + producerId);
        }
        long assigned;
        lock.lock();
        try {
            controlAppendGate.ensureControlWritable();
            var active = segments.get(segments.size() - 1);
            active.appendControl(nowMillis, producerId, producerEpoch, partitionLeaderEpoch, marker);
            assigned = active.nextOffset() - 1;
            txnState.onControl(producerId, marker.type());
            if (active.sizeBytes() >= segmentBytes) {
                var next = LogSegment.open(dir, active.nextOffset(), config.indexIntervalBytes());
                active.force();
                segments.add(next);
                markFlushedLocked();
            } else {
                onAppendedLocked(1, System.currentTimeMillis());
            }
        } finally {
            lock.unlock();
        }
        notifyAppend();
        return assigned;
    }

    /**
     * The last stable offset: every offset below it is transactionally
     * decided AND below the caller-provided replication boundary
     * {@code hwm} — {@code min(firstOffsetOfEarliestOngoingTxn, hwm)}.
     * Storage does not know the high watermark; the replication layer owns
     * that boundary and passes it in.
     */
    public long lastStableOffset(long hwm) {
        return txnState.lastStableOffset(hwm);
    }

    /** First offset of the earliest ongoing transaction, if any. */
    public java.util.OptionalLong firstOngoingTxnOffset() {
        return txnState.firstOngoingOffset();
    }

    /**
     * Aborted transactions overlapping the half-open fetch window
     * {@code [fetchStart, fetchEnd)}, sorted by first offset — what a
     * read-committed fetch response attaches so consumers can drop aborted
     * records.
     */
    public List<TransactionState.AbortedTxn> abortedTxnsIn(long fetchStart, long fetchEnd) {
        return txnState.abortedTxnsIn(fetchStart, fetchEnd);
    }

    public long nextOffset() {
        lock.lock();
        try {
            var active = segments.get(segments.size() - 1);
            return active.nextOffset();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Truncate the log so its LEO becomes {@code targetOffset}. Used by the
     * follower reconciliation path: after {@code OffsetsForLeaderEpoch}
     * reveals a divergent suffix, the follower drops everything at or after
     * {@code targetOffset} and re-fetches from there.
     *
     * <p>Batch-granular: a mid-batch targetOffset rounds down to the
     * containing batch's baseOffset (the whole divergent batch is dropped).
     */
    public void truncateTo(long targetOffset) throws IOException {
        lock.lock();
        try {
            if (targetOffset >= segments.get(segments.size() - 1).nextOffset()) return;
            while (segments.size() > 1 && segments.get(segments.size() - 1).baseOffset() >= targetOffset) {
                var tail = segments.remove(segments.size() - 1);
                tail.close();
                tail.delete();
            }
            var active = segments.get(segments.size() - 1);
            active.truncateAtOrAbove(targetOffset);
            // The dropped suffix may have held control batches (a decided
            // transaction becomes ongoing again) or transactional data (an
            // aborted range shrinks or vanishes) — recompute from what
            // survived. Rare path: only follower reconciliation truncates.
            rebuildTxnStateLocked();
        } finally {
            lock.unlock();
        }
    }

    /** Recompute {@link #txnState} from the surviving batches. Callers hold {@link #lock}. */
    private void rebuildTxnStateLocked() throws IOException {
        txnState.clear();
        for (var seg : segments) {
            long pos = seg.baseOffset();
            long limit = seg.nextOffset();
            while (pos < limit) {
                var batches = seg.readFrom(pos, 1 << 20);
                if (batches.isEmpty()) break;
                for (var b : batches) {
                    if (b.control() && b.producerId() >= 0) {
                        txnState.onControl(b.producerId(), b.controlRecord().type());
                    } else if (b.transactional() && b.producerId() >= 0) {
                        txnState.onTransactionalData(b.producerId(), b.baseOffset(), b.lastOffset());
                    }
                    pos = b.lastOffset() + 1;
                }
            }
        }
    }

    /**
     * Retry budget for reads racing a compaction segment swap. Reads
     * resolve their segment lock-free, and {@code compactByKeyLocked}
     * closes the old segments before publishing the replacement — a reader
     * in that window sees {@link java.nio.channels.ClosedChannelException}.
     * The window is file-create + one append (survivors are pre-encoded),
     * so a short park-and-re-resolve loop rides it out; exhausting the
     * ~200ms budget means a real IO problem and the last failure is
     * rethrown.
     */
    private static final int STALE_SEGMENT_RETRIES = 100;

    private static final long STALE_SEGMENT_PARK_NANOS = 2_000_000L; // 2ms

    /**
     * Read one or more batches starting at or after {@code offset}, capped at
     * {@code maxBytes}. Reads may span at most one segment boundary per call.
     */
    public List<RecordBatch.Parsed> read(long offset, int maxBytes) throws IOException {
        java.nio.channels.ClosedChannelException stale = null;
        for (int attempt = 0; attempt < STALE_SEGMENT_RETRIES; attempt++) {
            var segment = segmentContaining(offset);
            if (segment == null) return List.of();
            try {
                return segment.readFrom(offset, maxBytes);
            } catch (java.nio.channels.ClosedChannelException e) {
                stale = e; // compaction is swapping the segment list under us
                java.util.concurrent.locks.LockSupport.parkNanos(STALE_SEGMENT_PARK_NANOS);
            }
        }
        throw stale;
    }

    /** Zero-copy transfer to the given output stream. */
    public long transferTo(long offset, int maxBytes, OutputStream out) throws IOException {
        java.nio.channels.ClosedChannelException stale = null;
        for (int attempt = 0; attempt < STALE_SEGMENT_RETRIES; attempt++) {
            var segment = segmentContaining(offset);
            if (segment == null) return 0;
            try {
                return segment.transferTo(offset, maxBytes, out);
            } catch (java.nio.channels.ClosedChannelException e) {
                stale = e;
                java.util.concurrent.locks.LockSupport.parkNanos(STALE_SEGMENT_PARK_NANOS);
            }
        }
        throw stale;
    }

    private LogSegment segmentContaining(long offset) {
        LogSegment candidate = null;
        for (var seg : segments) {
            if (seg.baseOffset() <= offset) candidate = seg;
            else break;
        }
        // If the requested offset is below every segment's base
        // (e.g. after sparse-offset compaction a consumer still holds a
        // pre-compaction offset), return the earliest available segment
        // instead of nothing so the fetch path surfaces records from the
        // new logStartOffset rather than silently returning empty.
        if (candidate == null && !segments.isEmpty()) {
            candidate = segments.get(0);
        }
        return candidate;
    }

    /**
     * Delete closed segments whose max timestamp is older than
     * {@code cutoffMillis}. The active segment is never deleted. Returns the
     * number of segments removed.
     */
    public int retain(long cutoffMillis) throws IOException {
        return retain(cutoffMillis, -1L);
    }

    /**
     * Time- and size-based retention in one pass. First deletes closed
     * segments whose max timestamp is older than {@code cutoffMillis}, then
     * deletes closed segments from the head while doing so still leaves at
     * least {@code retentionBytes} in the log — so the log converges to
     * {@code [retentionBytes, retentionBytes + segmentBytes)} rather than
     * undershooting the limit by a whole segment. A negative
     * {@code retentionBytes} disables the size pass (callers express
     * time-unlimited retention by passing {@link Long#MIN_VALUE} as the
     * cutoff). The active segment is never deleted. Returns the number of
     * segments removed.
     */
    public int retain(long cutoffMillis, long retentionBytes) throws IOException {
        lock.lock();
        try {
            int removed = 0;
            while (segments.size() > 1) {
                var head = segments.get(0);
                if (head.maxTimestamp() > 0 && head.maxTimestamp() >= cutoffMillis) break;
                if (head == segments.get(segments.size() - 1)) break;
                segments.remove(0);
                head.close();
                head.delete();
                removed++;
            }
            if (retentionBytes >= 0) {
                long totalBytes = 0;
                for (var seg : segments) totalBytes += seg.sizeBytes();
                while (segments.size() > 1) {
                    var head = segments.get(0);
                    long headBytes = head.sizeBytes();
                    if (totalBytes - headBytes < retentionBytes) break;
                    segments.remove(0);
                    head.close();
                    head.delete();
                    totalBytes -= headBytes;
                    removed++;
                }
            }
            if (removed > 0) {
                // Aborted ranges wholly below the new log start can never be
                // fetched again — drop them so the index stays bounded by
                // what the log holds. Ongoing transactions are kept even if
                // retention ate their first batches: they are still
                // undecided, and the LSO must keep saying so.
                txnState.evictAbortedBelow(segments.get(0).baseOffset());
            }
            return removed;
        } finally {
            lock.unlock();
        }
    }

    public List<LogSegment> segments() {
        return List.copyOf(segments);
    }

    public void force() throws IOException {
        lock.lock();
        try {
            for (var seg : segments) seg.force();
            markFlushedLocked();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Synchronous log compaction. For each key, keeps only the record
     * with the highest offset; a record with {@code value == null} is a
     * tombstone and removes the key entirely. Records with {@code key == null}
     * are passed through unchanged.
     *
     * <p>Implementation is stop-the-world: reads every batch into memory,
     * dedups, rewrites a single new segment, swaps in place. Good enough for
     * the compaction-scale target (1M records, 1000 keys, laptop-sized). A streaming
     * cleaner with segment-granular swaps is a future optimisation.
     *
     * <p>Returns the number of records retained after compaction.
     */
    public int compactByKey() throws IOException {
        lock.lock();
        try {
            return compactByKeyLocked();
        } finally {
            lock.unlock();
        }
    }

    private int compactByKeyLocked() throws IOException {
        // Kafka-spec sparse-offset compaction. Scan every batch in
        // original order tracking the ABSOLUTE offset of each record. For
        // keyed records, keep the latest surviving offset per key; a
        // tombstone drops the key entirely. Null-keyed records are
        // pass-through and preserve their original offset. The resulting
        // compacted batch retains each survivor at its original absolute
        // offset by encoding the right offsetDelta values — consumers
        // requesting a compacted-away offset will land on the next batch
        // whose lastOffset ≥ target (standard Kafka fetch semantics).
        //
        // Concretely: if offsets 0,1,2,3,4 had keys A,B,A,C,B then
        // survivors are {2:A, 3:C, 4:B}; the compacted single-batch log has
        // base=2 and records with deltas [0,1,2] → absolute offsets 2,3,4.
        // If offsets were 0,1,5 with keys A,A,B then survivors are {1:A,
        // 5:B}; compacted batch has base=1 and deltas [0,4] — the gap at
        // offsets 2–4 is real, readers fetching offset=3 land on the batch
        // at base=1 (whose lastOffset=5).
        var byKey = new java.util.HashMap<java.nio.ByteBuffer, Long>(); // keyBytes -> abs offset of current winner
        var survivors =
                new java.util.TreeMap<Long, Record>(); // abs offset -> surviving record (naked, no offset delta)
        long nowMillis = System.currentTimeMillis();
        long firstTimestamp = nowMillis;
        long maxTimestamp = nowMillis;
        boolean sawAny = false;
        // Rewriting survivors of compressed batches keeps them compressed.
        var codec = Compression.NONE;
        for (var seg : segments) {
            long pos = seg.baseOffset();
            long limit = seg.nextOffset();
            while (pos < limit) {
                var batches = seg.readFrom(pos, 64 * 1024);
                if (batches.isEmpty()) break;
                for (var b : batches) {
                    if (b.control()) {
                        // The single-batch rewrite cannot preserve marker
                        // positions relative to the data they decide, and
                        // dropping markers would resurrect decided
                        // transactions. Transactional logs stay on delete
                        // retention until the cleaner understands markers.
                        throw new IOException("cannot compact " + dir
                                + ": log contains transaction control batches; compaction is unsupported for"
                                + " transactional logs");
                    }
                    if (!sawAny) {
                        firstTimestamp = b.firstTimestamp();
                        sawAny = true;
                    }
                    if (b.codec() != Compression.NONE) codec = b.codec();
                    if (b.maxTimestamp() > maxTimestamp) maxTimestamp = b.maxTimestamp();
                    long batchBase = b.baseOffset();
                    for (var r : b.records()) {
                        long absOff = batchBase + r.offsetDelta();
                        if (r.key() == null) {
                            // Null-keyed passthrough — retained at original offset.
                            survivors.put(absOff, new Record(0, 0L, null, r.value()));
                            continue;
                        }
                        var keyBuf = java.nio.ByteBuffer.wrap(r.key());
                        if (r.value() == null) {
                            // Tombstone — drop any prior winner for this key.
                            var prior = byKey.remove(keyBuf);
                            if (prior != null) survivors.remove(prior);
                            continue;
                        }
                        var prior = byKey.get(keyBuf);
                        if (prior != null) survivors.remove(prior);
                        byKey.put(keyBuf, absOff);
                        survivors.put(absOff, new Record(0, 0L, r.key(), r.value()));
                    }
                    pos = b.lastOffset() + 1;
                }
            }
        }

        // Encode the replacement batch BEFORE touching existing segments.
        // Readers resolve segments lock-free, so everything between the
        // first close() and the fresh segment's publication is a window
        // where they see closed channels; doing the (relatively) expensive
        // survivor encoding up front keeps that window to file-create plus
        // one append, which the read paths' bounded stale-segment retry
        // rides out. Records carry (absOff - firstOff) as their offsetDelta
        // — sparse gaps within a batch are well-formed per the RecordBatch
        // spec and survive the decode round-trip.
        java.util.ArrayList<Record> encoded = null;
        long firstOff = 0L;
        if (!survivors.isEmpty()) {
            firstOff = survivors.firstKey();
            encoded = new java.util.ArrayList<>(survivors.size());
            for (var entry : survivors.entrySet()) {
                long absOff = entry.getKey();
                var r = entry.getValue();
                encoded.add(new Record((int) (absOff - firstOff), 0L, r.key(), r.value()));
            }
        }

        // Swap: close + delete existing segments, then publish the
        // replacement segment at the first-surviving absolute offset.
        // (Deleting first also guarantees the fresh segment's base-offset
        // file names can't collide with an old segment's.)
        for (var seg : segments) {
            seg.close();
            seg.delete();
        }
        segments.clear();

        if (encoded == null) {
            // No records survived. Fresh empty segment at offset 0; nextOffset
            // resets to 0 (loss of historical offsets is acceptable when the
            // partition is empty — there are no consumers to surprise).
            segments.add(LogSegment.open(dir, 0L, config.indexIntervalBytes()));
            return 0;
        }

        var fresh = LogSegment.open(dir, firstOff, config.indexIntervalBytes());
        fresh.append(firstTimestamp, maxTimestamp, encoded, codec);
        // Publish only after the survivors are written so a racing reader
        // never observes a half-built segment.
        segments.add(fresh);
        return survivors.size();
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            for (var seg : segments) seg.close();
        } finally {
            lock.unlock();
        }
    }
}
