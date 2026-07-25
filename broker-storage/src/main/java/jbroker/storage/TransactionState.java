package jbroker.storage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;

/**
 * Per-log transaction bookkeeping: which producers have an open
 * transaction (and where it started), and which offset ranges belong to
 * aborted transactions. A transactional data batch opens or extends its
 * producer's ongoing range; a control batch for that producer closes it —
 * ABORT additionally records the range so fetches can hand consumers the
 * aborted spans overlapping their window.
 *
 * <p>Held in memory per log, rebuilt from the open-time recovery scan
 * (which already decodes every batch) — no sidecar file. Bounded: the
 * aborted list is evicted as retention advances the log start, and the
 * ongoing map holds at most one entry per producer with an open
 * transaction.
 *
 * <p>Thread-safety: all methods synchronize on this instance. Mutations
 * arrive from the log's append/truncate paths (already serialized by the
 * log lock); reads ({@link #lastStableOffset}, {@link #abortedTxnsIn})
 * arrive lock-free from fetch paths.
 */
public final class TransactionState {

    /**
     * An aborted transaction's data range: {@code [firstOffset, lastOffset]}
     * covers the producer's transactional data batches, not the marker
     * itself (markers are excluded from application-facing decode anyway).
     */
    public record AbortedTxn(long producerId, long firstOffset, long lastOffset) {}

    /**
     * One recovery-scan observation, in log order. {@code marker == null}
     * means a transactional data batch spanning
     * {@code [baseOffset, lastOffset]}; otherwise a control batch at
     * {@code baseOffset} carrying {@code marker}.
     */
    public record TxnEvent(long producerId, long baseOffset, long lastOffset, ControlRecord.Type marker) {

        public static TxnEvent data(long producerId, long baseOffset, long lastOffset) {
            return new TxnEvent(producerId, baseOffset, lastOffset, null);
        }

        public static TxnEvent control(long producerId, long offset, ControlRecord.Type marker) {
            return new TxnEvent(producerId, offset, offset, marker);
        }
    }

    private static final class Ongoing {
        final long firstOffset;
        long lastOffset;

        Ongoing(long firstOffset, long lastOffset) {
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
        }
    }

    private final Map<Long, Ongoing> ongoing = new HashMap<>();

    /** firstOffset → producerId; distinct producers open at distinct batch offsets. */
    private final TreeMap<Long, Long> ongoingByFirstOffset = new TreeMap<>();

    private final List<AbortedTxn> aborted = new ArrayList<>();

    /** A transactional data batch for {@code producerId} opened or extended its transaction. */
    public synchronized void onTransactionalData(long producerId, long baseOffset, long lastOffset) {
        if (producerId < 0) {
            throw new IllegalArgumentException("transactional batch requires producerId >= 0, got " + producerId);
        }
        var open = ongoing.get(producerId);
        if (open == null) {
            ongoing.put(producerId, new Ongoing(baseOffset, lastOffset));
            ongoingByFirstOffset.put(baseOffset, producerId);
        } else if (lastOffset > open.lastOffset) {
            open.lastOffset = lastOffset;
        }
    }

    /**
     * A control batch for {@code producerId} closed its transaction. A
     * marker with no ongoing transaction is a no-op — the transaction
     * carried no data batches in this log, so there is nothing to decide.
     */
    public synchronized void onControl(long producerId, ControlRecord.Type marker) {
        var open = ongoing.remove(producerId);
        if (open == null) return;
        ongoingByFirstOffset.remove(open.firstOffset);
        if (marker == ControlRecord.Type.ABORT) {
            aborted.add(new AbortedTxn(producerId, open.firstOffset, open.lastOffset));
        }
    }

    /** Replay one recovery-scan event. Data events without producer identity are ignored. */
    public void apply(TxnEvent event) {
        if (event.marker() != null) {
            onControl(event.producerId(), event.marker());
        } else if (event.producerId() >= 0) {
            onTransactionalData(event.producerId(), event.baseOffset(), event.lastOffset());
        }
    }

    /**
     * The last stable offset given the caller's high-watermark-equivalent
     * boundary: every offset below it is replicated (per {@code hwm}) and
     * transactionally decided. Exclusive, like the boundary itself —
     * {@code min(firstOffsetOfEarliestOngoingTxn, hwm)}; equals {@code hwm}
     * when no transaction is ongoing.
     */
    public synchronized long lastStableOffset(long hwm) {
        if (ongoingByFirstOffset.isEmpty()) return hwm;
        return Math.min(ongoingByFirstOffset.firstKey(), hwm);
    }

    /** First offset of the earliest ongoing transaction, if any. */
    public synchronized OptionalLong firstOngoingOffset() {
        return ongoingByFirstOffset.isEmpty() ? OptionalLong.empty() : OptionalLong.of(ongoingByFirstOffset.firstKey());
    }

    /** Number of producers with an ongoing transaction. */
    public synchronized int ongoingCount() {
        return ongoing.size();
    }

    /**
     * Aborted transactions overlapping the half-open fetch window
     * {@code [fetchStart, fetchEnd)}, sorted by first offset. A consumer
     * fetching that window drops records of these producers from each
     * returned {@code firstOffset} up to the producer's abort marker.
     */
    public synchronized List<AbortedTxn> abortedTxnsIn(long fetchStart, long fetchEnd) {
        var out = new ArrayList<AbortedTxn>();
        for (var txn : aborted) {
            if (txn.firstOffset() < fetchEnd && txn.lastOffset() >= fetchStart) out.add(txn);
        }
        out.sort(Comparator.comparingLong(AbortedTxn::firstOffset));
        return out;
    }

    /**
     * Drop aborted transactions that ended below {@code logStartOffset} —
     * retention deleted their data, so no fetch can return it. Keeps the
     * in-memory index bounded by what the log still holds.
     */
    public synchronized void evictAbortedBelow(long logStartOffset) {
        aborted.removeIf(txn -> txn.lastOffset() < logStartOffset);
    }

    /** Forget everything; used before a full rebuild (truncation). */
    public synchronized void clear() {
        ongoing.clear();
        ongoingByFirstOffset.clear();
        aborted.clear();
    }
}
