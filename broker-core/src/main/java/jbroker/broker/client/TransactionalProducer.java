package jbroker.broker.client;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import jbroker.broker.ErrorCodes;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;
import jbroker.proto.txn.AddOffsetsToTxnRequest;
import jbroker.proto.txn.AddPartitionsToTxnRequest;
import jbroker.proto.txn.EndTxnRequest;
import jbroker.proto.txn.InitTransactionsRequest;
import jbroker.proto.txn.TxnOffsetCommitEntry;
import jbroker.proto.txn.TxnOffsetCommitRequest;
import jbroker.storage.Compression;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;

/**
 * Transactional producer over {@link ClusterClient}: atomic multi-partition
 * produces plus consumer-offset commits, the client half of
 * consume-transform-produce exactly-once.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@link #initTransactions()} — registers the transactional id with
 *       its coordinator and adopts the granted {@code (producerId, epoch)}.
 *       Every re-init bumps the epoch, fencing zombies holding the old one
 *       and aborting whatever they left in flight.</li>
 *   <li>{@link #beginTransaction()} — opens a transaction locally
 *       (nothing on the wire; the coordinator opens it on the first
 *       partition registration).</li>
 *   <li>{@link #send} — registers the partition via
 *       {@code AddPartitionsToTxn} on first touch, then produces the
 *       record as a TRANSACTIONAL idempotent batch (acks=all) under the
 *       granted identity.</li>
 *   <li>{@link #sendOffsetsToTransaction} — {@code AddOffsetsToTxn} (once
 *       per group per transaction) + {@code TxnOffsetCommit}: the group's
 *       offsets commit or vanish atomically with the produced data.</li>
 *   <li>{@link #commitTransaction()} / {@link #abortTransaction()} —
 *       {@code EndTxn}. {@code CONCURRENT_TRANSACTIONS} (previous
 *       transaction's markers still in flight) retries with backoff;
 *       {@link ProducerFencedException} is fatal.</li>
 * </ol>
 *
 * <p>{@link #transact(Runnable)} wraps a whole attempt in the
 * transactional contract's abort-and-retry loop: the body runs between
 * begin and commit, any abortable failure aborts the attempt, re-inits
 * (the epoch bump both fences the aborted attempt's broker-side residue
 * and resets sequences, so a resend can never be silently deduped against
 * aborted data), and re-runs the body. Aborted attempts are invisible to
 * read_committed consumers, so the retry is exactly-once end to end.
 * A failure of the COMMIT itself is never retried into an abort — the
 * decision may already be logged coordinator-side and a decision is never
 * reversed — it propagates to the caller.
 *
 * <p>Not thread-safe beyond its own synchronization: one in-flight
 * transaction per producer, driven from one application thread (methods
 * are synchronized so misuse fails coherently rather than corrupting
 * sequence state). Batching integration is out of scope — sends are
 * single-record blocking calls.
 */
public final class TransactionalProducer implements AutoCloseable {

    /**
     * The coordinator granted this transactional id's identity to a newer
     * producer (or the transaction timed out and was force-aborted with an
     * epoch bump). Fatal: this instance can never make progress again —
     * the application must build a new producer (or accept that a newer
     * instance owns the id).
     */
    public static final class ProducerFencedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        ProducerFencedException(String message) {
            super(message);
        }
    }

    /**
     * Tuning knobs.
     *
     * @param transactionTimeoutMs how long the coordinator lets a
     *     transaction sit without progress before force-aborting it and
     *     fencing this producer. Must comfortably exceed the longest stall
     *     the application tolerates mid-transaction (leader failover under
     *     acks=all can stall produces for tens of seconds). 0 selects the
     *     broker default.
     * @param retryBackoffMs pause between CONCURRENT_TRANSACTIONS retries
     *     and between {@link #transact} attempts.
     * @param opDeadlineMs per-operation deadline (send, offset commit,
     *     end-txn, init).
     * @param transactDeadlineMs overall budget for one {@link #transact}
     *     call across all its abort-and-retry attempts.
     */
    public record Config(int transactionTimeoutMs, long retryBackoffMs, long opDeadlineMs, long transactDeadlineMs) {
        public Config {
            if (retryBackoffMs < 1) throw new IllegalArgumentException("retryBackoffMs must be positive");
            if (opDeadlineMs < 1) throw new IllegalArgumentException("opDeadlineMs must be positive");
            if (transactDeadlineMs < 1) throw new IllegalArgumentException("transactDeadlineMs must be positive");
        }

        public static Config defaults() {
            return new Config(
                    /*transactionTimeoutMs*/ 0, /*retryBackoffMs*/
                    50, /*opDeadlineMs*/
                    120_000,
                    /*transactDeadlineMs*/ 600_000);
        }
    }

    private enum State {
        UNINITIALIZED,
        READY,
        IN_TRANSACTION
    }

    /** Sleep seam so unit tests can drive backoffs without wall-clock time. */
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private record Tp(String topic, int partition) {}

    private final ClusterClient cluster;
    private final String transactionalId;
    private final Config config;
    private final Sleeper sleeper;

    private State state = State.UNINITIALIZED;
    private long producerId = -1L;
    private int producerEpoch = -1;
    /** Next sequence per partition under the CURRENT epoch; epoch bumps reset it. */
    private final Map<Tp, Integer> nextSequence = new HashMap<>();
    /** Partitions registered in the current transaction. */
    private final Set<Tp> registeredPartitions = new HashSet<>();
    /** Groups whose offsets partition is registered in the current transaction. */
    private final Set<String> registeredOffsetGroups = new HashSet<>();

    private boolean closed;

    public TransactionalProducer(ClusterClient cluster, String transactionalId) {
        this(cluster, transactionalId, Config.defaults());
    }

    public TransactionalProducer(ClusterClient cluster, String transactionalId, Config config) {
        this(cluster, transactionalId, config, Thread::sleep);
    }

    TransactionalProducer(ClusterClient cluster, String transactionalId, Config config, Sleeper sleeper) {
        if (transactionalId == null || transactionalId.isEmpty()) {
            throw new IllegalArgumentException("transactionalId must be non-empty");
        }
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.transactionalId = transactionalId;
        this.config = Objects.requireNonNull(config, "config");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    }

    /** The granted producer id, or -1 before {@link #initTransactions()}. Test/inspection accessor. */
    public synchronized long producerId() {
        return producerId;
    }

    /** The granted producer epoch, or -1 before {@link #initTransactions()}. Test/inspection accessor. */
    public synchronized int producerEpoch() {
        return producerEpoch;
    }

    /**
     * Register with the coordinator and adopt the granted identity. Legal
     * from any state — a re-init mid-transaction abandons that transaction
     * (the coordinator aborts it as a side effect of the epoch bump).
     * Retries CONCURRENT_TRANSACTIONS (the previous transaction's markers
     * are still being delivered) with backoff until the op deadline.
     */
    public synchronized void initTransactions() {
        ensureOpen();
        long deadline = deadlineNanos(config.opDeadlineMs());
        while (true) {
            var resp = cluster.initTransactions(
                    InitTransactionsRequest.newBuilder()
                            .setTransactionalId(transactionalId)
                            .setTransactionTimeoutMs(config.transactionTimeoutMs())
                            .build(),
                    config.opDeadlineMs());
            switch (resp.getError()) {
                case OK -> {
                    producerId = resp.getProducerId();
                    producerEpoch = resp.getProducerEpoch();
                    nextSequence.clear();
                    registeredPartitions.clear();
                    registeredOffsetGroups.clear();
                    state = State.READY;
                    return;
                }
                case CONCURRENT_TRANSACTIONS -> backoff(deadline, "initTransactions");
                default -> throw failure("initTransactions", resp.getError());
            }
        }
    }

    /** Open a transaction. Requires a successful {@link #initTransactions()} and no transaction in flight. */
    public synchronized void beginTransaction() {
        ensureOpen();
        if (state == State.UNINITIALIZED) {
            throw new IllegalStateException("initTransactions() must succeed before beginTransaction()");
        }
        if (state == State.IN_TRANSACTION) {
            throw new IllegalStateException("a transaction is already in flight");
        }
        registeredPartitions.clear();
        registeredOffsetGroups.clear();
        state = State.IN_TRANSACTION;
    }

    /**
     * Produce one record inside the current transaction: registers the
     * partition on first touch, then sends a TRANSACTIONAL idempotent
     * batch (acks=all) under the granted identity — retries of the same
     * sequence dedupe broker-side. Returns the record's absolute offset.
     */
    public synchronized long send(String topic, int partition, byte[] value) {
        ensureInTransaction("send");
        var tp = new Tp(topic, partition);
        if (!registeredPartitions.contains(tp)) {
            registerPartition(tp);
        }
        int sequence = nextSequence.getOrDefault(tp, 0);
        var records = List.of(new Record(0, 0L, null, value));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        long now = System.currentTimeMillis();
        RecordBatch.encode(
                buf, 0L, 0, now, now, producerId, (short) producerEpoch, sequence, records, Compression.NONE, true);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        var req = ProduceRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setBatch(ByteString.copyFrom(bytes))
                .setProducerId(producerId)
                .setProducerEpoch(producerEpoch)
                .setBaseSequence(sequence)
                .setAcks(-1)
                .build();
        long lastOffset;
        try {
            lastOffset = cluster.produce(req, config.opDeadlineMs()).getLastOffset();
        } catch (ClusterClient.BrokerErrorException e) {
            if (e.code() == ErrorCodes.PRODUCER_FENCED) {
                throw fenced("send to " + topic + "-" + partition);
            }
            throw e;
        }
        nextSequence.put(tp, sequence + 1);
        return lastOffset;
    }

    /**
     * Commit {@code offsets} for {@code groupId} as part of the current
     * transaction: {@code AddOffsetsToTxn} registers the group's offsets
     * partition (once per transaction), {@code TxnOffsetCommit} stages the
     * offsets with the group coordinator. They become visible to
     * {@code FetchOffsets} only when the transaction commits; an abort
     * discards them.
     */
    public synchronized void sendOffsetsToTransaction(String groupId, Map<TopicPartition, Long> offsets) {
        ensureInTransaction("sendOffsetsToTransaction");
        if (offsets.isEmpty()) return;
        if (!registeredOffsetGroups.contains(groupId)) {
            registerOffsetGroup(groupId);
        }
        var b = TxnOffsetCommitRequest.newBuilder()
                .setTransactionalId(transactionalId)
                .setGroupId(groupId)
                .setProducerId(producerId)
                .setProducerEpoch(producerEpoch);
        for (var e : offsets.entrySet()) {
            b.addOffsets(TxnOffsetCommitEntry.newBuilder()
                    .setTp(e.getKey())
                    .setOffset(e.getValue())
                    .build());
        }
        var resp = cluster.txnOffsetCommit(b.build(), config.opDeadlineMs());
        for (var result : resp.getResultsList()) {
            if (result.getError() == ErrorCode.OK) continue;
            if (result.getError() == ErrorCode.PRODUCER_FENCED) {
                throw fenced("sendOffsetsToTransaction for group " + groupId);
            }
            throw failure("sendOffsetsToTransaction for group " + groupId, result.getError());
        }
    }

    /**
     * Commit the current transaction. Returns once the coordinator has
     * durably logged the decision; marker delivery continues broker-side
     * and decides visibility. A transaction that touched nothing commits
     * locally without a round trip.
     */
    public synchronized void commitTransaction() {
        ensureInTransaction("commitTransaction");
        endCurrentTransaction(true);
    }

    /** Abort the current transaction; staged offsets and produced records become permanently invisible. */
    public synchronized void abortTransaction() {
        ensureInTransaction("abortTransaction");
        endCurrentTransaction(false);
    }

    /**
     * Run one transaction with the contract's abort-and-retry loop:
     * begin, run {@code body}, commit. An abortable failure inside the
     * body aborts the attempt, re-inits (epoch bump — see the class
     * javadoc for why the bump is what makes the retry exactly-once), and
     * re-runs the body after a backoff. {@link ProducerFencedException}
     * and commit failures propagate.
     */
    public void transact(Runnable body) {
        synchronized (this) {
            ensureOpen();
            if (state == State.UNINITIALIZED) {
                throw new IllegalStateException("initTransactions() must succeed before transact()");
            }
        }
        long deadline = deadlineNanos(config.transactDeadlineMs());
        RuntimeException last = null;
        while (true) {
            if (System.nanoTime() >= deadline) {
                throw new RuntimeException("transact: retries exhausted for '" + transactionalId + "'", last);
            }
            beginTransaction();
            try {
                body.run();
            } catch (ProducerFencedException e) {
                throw e;
            } catch (RuntimeException e) {
                last = e;
                abortAndReinit();
                backoff(deadline, "transact retry");
                continue;
            }
            commitTransaction();
            return;
        }
    }

    /**
     * Best-effort abort of an in-flight transaction, then release. The
     * {@link ClusterClient} is caller-owned and stays open.
     */
    @Override
    public synchronized void close() {
        if (closed) return;
        if (state == State.IN_TRANSACTION) {
            try {
                endCurrentTransaction(false);
            } catch (RuntimeException ignored) {
                // The transaction timeout sweep aborts it coordinator-side.
            }
        }
        closed = true;
    }

    // ---- internals ----

    private void registerPartition(Tp tp) {
        long deadline = deadlineNanos(config.opDeadlineMs());
        while (true) {
            var resp = cluster.addPartitionsToTxn(
                    AddPartitionsToTxnRequest.newBuilder()
                            .setTransactionalId(transactionalId)
                            .setProducerId(producerId)
                            .setProducerEpoch(producerEpoch)
                            .addPartitions(TopicPartition.newBuilder()
                                    .setTopic(tp.topic())
                                    .setPartition(tp.partition())
                                    .build())
                            .build(),
                    config.opDeadlineMs());
            switch (resp.getError()) {
                case OK -> {
                    registeredPartitions.add(tp);
                    return;
                }
                case CONCURRENT_TRANSACTIONS -> backoff(deadline, "addPartitionsToTxn");
                case PRODUCER_FENCED -> throw fenced("addPartitionsToTxn " + tp.topic() + "-" + tp.partition());
                default -> throw failure("addPartitionsToTxn " + tp.topic() + "-" + tp.partition(), resp.getError());
            }
        }
    }

    private void registerOffsetGroup(String groupId) {
        long deadline = deadlineNanos(config.opDeadlineMs());
        while (true) {
            var resp = cluster.addOffsetsToTxn(
                    AddOffsetsToTxnRequest.newBuilder()
                            .setTransactionalId(transactionalId)
                            .setProducerId(producerId)
                            .setProducerEpoch(producerEpoch)
                            .setGroupId(groupId)
                            .build(),
                    config.opDeadlineMs());
            switch (resp.getError()) {
                case OK -> {
                    registeredOffsetGroups.add(groupId);
                    return;
                }
                case CONCURRENT_TRANSACTIONS -> backoff(deadline, "addOffsetsToTxn");
                case PRODUCER_FENCED -> throw fenced("addOffsetsToTxn for group " + groupId);
                default -> throw failure("addOffsetsToTxn for group " + groupId, resp.getError());
            }
        }
    }

    private void endCurrentTransaction(boolean commit) {
        String what = (commit ? "commit" : "abort") + "Transaction";
        if (registeredPartitions.isEmpty() && registeredOffsetGroups.isEmpty()) {
            // Nothing was registered, so the coordinator never opened the
            // transaction — EndTxn would answer INVALID_TXN_STATE.
            state = State.READY;
            return;
        }
        long deadline = deadlineNanos(config.opDeadlineMs());
        while (true) {
            var resp = cluster.endTxn(
                    EndTxnRequest.newBuilder()
                            .setTransactionalId(transactionalId)
                            .setProducerId(producerId)
                            .setProducerEpoch(producerEpoch)
                            .setCommit(commit)
                            .build(),
                    config.opDeadlineMs());
            switch (resp.getError()) {
                case OK -> {
                    state = State.READY;
                    return;
                }
                case CONCURRENT_TRANSACTIONS -> backoff(deadline, what);
                case PRODUCER_FENCED -> throw fenced(what);
                default -> throw failure(what, resp.getError());
            }
        }
    }

    /**
     * Abort the failed attempt and re-init. When the abort itself cannot
     * be confirmed, re-init alone is enough — the epoch bump aborts the
     * in-flight transaction coordinator-side.
     */
    private void abortAndReinit() {
        synchronized (this) {
            if (state == State.IN_TRANSACTION) {
                try {
                    endCurrentTransaction(false);
                } catch (ProducerFencedException e) {
                    throw e;
                } catch (RuntimeException abortFailure) {
                    // Fall through to re-init; see javadoc.
                    state = State.READY;
                }
            }
        }
        initTransactions();
    }

    private void ensureInTransaction(String what) {
        ensureOpen();
        if (state != State.IN_TRANSACTION) {
            throw new IllegalStateException(what + " requires beginTransaction() first");
        }
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("producer is closed");
    }

    private ProducerFencedException fenced(String what) {
        return new ProducerFencedException(
                what + ": producer fenced — a newer producer owns transactional id '" + transactionalId + "'");
    }

    private static RuntimeException failure(String what, ErrorCode error) {
        return new RuntimeException(what + " failed: " + error);
    }

    private static long deadlineNanos(long ms) {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ms);
    }

    private void backoff(long deadlineNanos, String what) {
        if (System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.retryBackoffMs()) >= deadlineNanos) {
            throw new RuntimeException(what + ": retries exhausted for '" + transactionalId + "'");
        }
        try {
            sleeper.sleep(config.retryBackoffMs());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(what + ": interrupted during backoff");
        }
    }
}
