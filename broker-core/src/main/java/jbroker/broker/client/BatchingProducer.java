package jbroker.broker.client;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import jbroker.storage.Compression;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;

/**
 * Async batching producer over {@link BrokerClient}. {@link #send} enqueues a
 * record and returns immediately with a future for its absolute offset; a
 * background sender thread packs records into per-(topic, partition) batches
 * and ships each one with a single acks=all RPC. A batch closes when its
 * encoded size reaches {@link Config#batchSizeBytes} or when
 * {@link Config#lingerMs} has elapsed since its first record — whichever
 * comes first.
 *
 * <p>Delivery is idempotent: the producer allocates a producer id lazily on
 * first use ({@link BatchSender#initProducerId}) and stamps every batch with a
 * per-partition base sequence. A failed RPC is retried with the SAME sequence,
 * which the broker dedupes, so a completed future means the records are on the
 * partition exactly once at the reported offsets. Retries back off
 * exponentially (starting at {@link Config#retryBackoffMs}, capped at 1s)
 * until {@link Config#deliveryTimeoutMs} has elapsed since the batch's first
 * record; past the deadline the batch's futures complete exceptionally.
 *
 * <p>Ordering: batches for one partition are sent strictly in accumulation
 * order, never pipelined — two in-flight batches for the same partition would
 * defeat the broker's contiguous-sequence dedup check. A single sender thread
 * enforces this globally (one RPC in flight at a time), which is plenty for
 * the client-side batching this class exists to provide.
 *
 * <p>Failure caveat: a batch that exhausts its delivery deadline leaves a gap
 * in the partition's sequence stream, and the broker will reject the
 * subsequent (now non-contiguous) batch as out-of-order. After a delivery
 * failure the producer should be closed and replaced — exactly-once
 * bookkeeping cannot resume across a hole it never delivered.
 */
public final class BatchingProducer implements AutoCloseable {

    /** Retry backoff doubles per attempt up to this ceiling. */
    private static final long MAX_BACKOFF_MS = 1_000;

    /**
     * Tuning knobs. {@link #defaults()} gives 64 KiB batches, 5 ms linger,
     * a 120 s delivery deadline, 100 ms initial retry backoff, and no
     * compression.
     *
     * <p>{@code compression} is applied when a batch is encoded for the
     * wire; {@code batchSizeBytes} accounts UNCOMPRESSED encoded bytes
     * (compressed size isn't known until send time), so a compressed
     * batch ships at or below the configured threshold.
     */
    public record Config(
            int batchSizeBytes, long lingerMs, long deliveryTimeoutMs, long retryBackoffMs, Compression compression) {

        public Config {
            if (batchSizeBytes < 1) throw new IllegalArgumentException("batchSizeBytes must be positive");
            if (lingerMs < 0) throw new IllegalArgumentException("lingerMs must be non-negative");
            if (deliveryTimeoutMs < 1) throw new IllegalArgumentException("deliveryTimeoutMs must be positive");
            if (retryBackoffMs < 1) throw new IllegalArgumentException("retryBackoffMs must be positive");
            Objects.requireNonNull(compression, "compression");
        }

        /** Compression-free variant — the pre-codec tuning surface. */
        public Config(int batchSizeBytes, long lingerMs, long deliveryTimeoutMs, long retryBackoffMs) {
            this(batchSizeBytes, lingerMs, deliveryTimeoutMs, retryBackoffMs, Compression.NONE);
        }

        public static Config defaults() {
            return new Config(64 * 1024, 5, 120_000, 100);
        }
    }

    /**
     * Transport seam. Production code adapts {@link BrokerClient} via
     * {@link #create}; unit tests substitute a fake so the accumulation,
     * ordering, and retry logic is exercised without a broker.
     */
    public interface BatchSender {

        /** Allocate the idempotent-producer id. Called once, before the first send. */
        long initProducerId();

        /**
         * Ship one batch and return the absolute offset of the LAST record.
         * Must throw a {@link RuntimeException} on failure; the producer
         * retries the same {@code baseSequence} until the delivery deadline.
         */
        long send(
                String topic, int partition, long producerId, int producerEpoch, int baseSequence, List<byte[]> values);
    }

    private final BatchSender sender;
    private final Config config;

    private final Object lock = new Object();

    /** Per-partition accumulation state; guarded by {@link #lock}. */
    private final Map<TopicPartition, PartitionState> partitions = new LinkedHashMap<>();

    /** Batch currently being sent, if any; guarded by {@link #lock}. */
    private Batch inFlight;

    /** Guarded by {@link #lock}; once set, {@link #send} rejects new records. */
    private boolean closed;

    /** Lazily allocated on first delivery; touched only by the sender thread. */
    private long producerId = -1;

    private final Thread senderThread;

    public BatchingProducer(BatchSender sender) {
        this(sender, Config.defaults());
    }

    public BatchingProducer(BatchSender sender, Config config) {
        this.sender = Objects.requireNonNull(sender, "sender");
        this.config = Objects.requireNonNull(config, "config");
        this.senderThread = new Thread(this::runSenderLoop, "batching-producer-sender");
        this.senderThread.setDaemon(true);
        this.senderThread.start();
    }

    /** Wire a producer to a live broker: idempotent, acks=all, default config. */
    public static BatchingProducer create(BrokerClient client) {
        return create(client, Config.defaults());
    }

    /**
     * Wire a producer to a whole cluster: idempotent, acks=all, and
     * failover-transparent. Batches route to the partition leader through
     * {@link ClusterClient}; on leader failover the SAME base sequence is
     * retried against the new leader, which either dedupes (the batch
     * replicated before the old leader died) or appends it fresh — so a
     * completed future still means exactly-once at the reported offsets,
     * with no error handling in the application.
     */
    public static BatchingProducer create(ClusterClient client) {
        return create(client, Config.defaults());
    }

    /** Cluster-routed producer with explicit tuning. */
    public static BatchingProducer create(ClusterClient client, Config config) {
        return new BatchingProducer(
                new BatchSender() {
                    @Override
                    public long initProducerId() {
                        return client.initProducerId();
                    }

                    @Override
                    public long send(
                            String topic,
                            int partition,
                            long producerId,
                            int producerEpoch,
                            int baseSequence,
                            List<byte[]> values) {
                        return client.produceIdempotentBatchAcksAll(
                                topic, partition, values, producerId, producerEpoch, baseSequence);
                    }
                },
                config);
    }

    /** Wire a producer to a live broker with explicit tuning. */
    public static BatchingProducer create(BrokerClient client, Config config) {
        return new BatchingProducer(
                new BatchSender() {
                    @Override
                    public long initProducerId() {
                        return client.initProducerId();
                    }

                    @Override
                    public long send(
                            String topic,
                            int partition,
                            long producerId,
                            int producerEpoch,
                            int baseSequence,
                            List<byte[]> values) {
                        return client.idempotentProduceBatchAcksAll(
                                topic,
                                partition,
                                values,
                                producerId,
                                producerEpoch,
                                baseSequence,
                                config.compression());
                    }
                },
                config);
    }

    /**
     * Enqueue one record. Returns a future that completes with the record's
     * absolute offset once its batch is committed (acks=all), or
     * exceptionally if the batch could not be delivered within the delivery
     * deadline. Never blocks on the network.
     */
    public CompletableFuture<Long> send(String topic, int partition, byte[] value) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(value, "value");
        if (partition < 0) throw new IllegalArgumentException("partition must be non-negative");
        var future = new CompletableFuture<Long>();
        synchronized (lock) {
            if (closed) throw new IllegalStateException("producer is closed");
            var ps = partitions.computeIfAbsent(new TopicPartition(topic, partition), tp -> new PartitionState());
            if (ps.open == null) {
                ps.open = new Batch(topic, partition, ps.nextSequence, System.nanoTime());
            }
            var batch = ps.open;
            batch.encodedBytes += encodedRecordSize(batch.values.size(), value);
            batch.values.add(value);
            batch.futures.add(future);
            ps.nextSequence++;
            // Size trigger: seal as soon as the encoded batch reaches the
            // threshold (an oversized single record simply travels alone).
            if (batch.encodedBytes >= config.batchSizeBytes()) {
                ps.sealed.addLast(batch);
                ps.open = null;
            }
            lock.notifyAll();
        }
        return future;
    }

    /**
     * Force every pending batch out and wait until each one has either
     * committed or failed. Does not throw on per-record failure — errors
     * live on the record futures. Records sent concurrently with a flush
     * may or may not be included in it.
     */
    public void flush() {
        var pending = new ArrayList<CompletableFuture<Long>>();
        synchronized (lock) {
            for (var ps : partitions.values()) {
                if (ps.open != null) {
                    ps.sealed.addLast(ps.open);
                    ps.open = null;
                }
                for (var batch : ps.sealed) {
                    pending.addAll(batch.futures);
                }
            }
            if (inFlight != null) {
                pending.addAll(inFlight.futures);
            }
            lock.notifyAll();
        }
        for (var f : pending) {
            f.exceptionally(e -> null).join();
        }
    }

    /**
     * Flush, then stop the sender thread. Idempotent. Records enqueued
     * after close() throws {@code IllegalStateException}; anything accepted
     * before is delivered (or failed) before the thread exits.
     */
    @Override
    public void close() {
        synchronized (lock) {
            if (closed) return;
        }
        flush();
        synchronized (lock) {
            closed = true;
            lock.notifyAll();
        }
        try {
            senderThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---- Sender thread ----

    private void runSenderLoop() {
        while (true) {
            Batch batch;
            synchronized (lock) {
                while (true) {
                    long now = System.nanoTime();
                    batch = pollDue(now);
                    if (batch != null) {
                        inFlight = batch;
                        break;
                    }
                    if (closed) return; // fully drained
                    try {
                        long waitNanos = nanosUntilNextLingerDeadline(now);
                        if (waitNanos == Long.MAX_VALUE) {
                            lock.wait();
                        } else {
                            // Round up: over-waiting a millisecond is harmless
                            // (the poll re-checks), wait(0) would sleep forever.
                            lock.wait(Math.max(1, TimeUnit.NANOSECONDS.toMillis(waitNanos) + 1));
                        }
                    } catch (InterruptedException e) {
                        // The sender thread is private to this producer and
                        // nothing interrupts it on purpose; treat like a
                        // spurious wakeup rather than abandoning pending
                        // futures.
                    }
                }
            }
            sendWithRetries(batch);
            synchronized (lock) {
                inFlight = null;
            }
        }
    }

    /**
     * Pick the next batch to send, oldest first: any sealed batch (closed by
     * size, flush, or shutdown) before any open batch whose linger expired.
     * Heads of the per-partition FIFO deques keep partition order intact.
     */
    private Batch pollDue(long nowNanos) {
        if (closed) {
            // Records accepted between flush() and the closed flag still
            // must go out; force-seal them so the drain below sees them.
            for (var ps : partitions.values()) {
                if (ps.open != null) {
                    ps.sealed.addLast(ps.open);
                    ps.open = null;
                }
            }
        }
        PartitionState best = null;
        for (var ps : partitions.values()) {
            var head = ps.sealed.peekFirst();
            if (head == null) continue;
            if (best == null || head.firstAppendNanos < best.sealed.peekFirst().firstAppendNanos) {
                best = ps;
            }
        }
        if (best != null) return best.sealed.pollFirst();

        long lingerNanos = TimeUnit.MILLISECONDS.toNanos(config.lingerMs());
        PartitionState duePs = null;
        for (var ps : partitions.values()) {
            var open = ps.open;
            if (open == null || nowNanos - open.firstAppendNanos < lingerNanos) continue;
            if (duePs == null || open.firstAppendNanos < duePs.open.firstAppendNanos) {
                duePs = ps;
            }
        }
        if (duePs != null) {
            var batch = duePs.open;
            duePs.open = null;
            return batch;
        }
        return null;
    }

    /** Nanos until the earliest open batch lingers out, or MAX_VALUE if none is open. */
    private long nanosUntilNextLingerDeadline(long nowNanos) {
        long lingerNanos = TimeUnit.MILLISECONDS.toNanos(config.lingerMs());
        long earliest = Long.MAX_VALUE;
        for (var ps : partitions.values()) {
            if (ps.open != null) {
                earliest = Math.min(earliest, ps.open.firstAppendNanos + lingerNanos - nowNanos);
            }
        }
        return earliest;
    }

    /**
     * Deliver one batch, retrying the SAME base sequence on any
     * RuntimeException until the delivery deadline (measured from the batch's
     * first record, so linger and queueing time count against it). On
     * success every record future completes with baseOffset + i; past the
     * deadline all futures complete exceptionally with the last failure as
     * cause. Never throws.
     */
    private void sendWithRetries(Batch batch) {
        long deadlineNanos = batch.firstAppendNanos + TimeUnit.MILLISECONDS.toNanos(config.deliveryTimeoutMs());
        long backoffMs = config.retryBackoffMs();
        RuntimeException last = null;
        while (true) {
            try {
                if (producerId < 0) {
                    producerId = sender.initProducerId();
                }
                long lastOffset = sender.send(
                        batch.topic, batch.partition, producerId, /*epoch*/ 0, batch.baseSequence, batch.values);
                long baseOffset = lastOffset - (batch.values.size() - 1);
                for (int i = 0; i < batch.futures.size(); i++) {
                    batch.futures.get(i).complete(baseOffset + i);
                }
                return;
            } catch (RuntimeException e) {
                last = e;
            }
            long now = System.nanoTime();
            if (now >= deadlineNanos) break;
            long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - now) + 1;
            try {
                Thread.sleep(Math.max(1, Math.min(backoffMs, remainingMs)));
            } catch (InterruptedException e) {
                break; // shutting down hard — fail the batch below
            }
            backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
        }
        var failure = new RuntimeException(
                "delivery deadline exceeded for " + batch.topic + "-" + batch.partition + " baseSequence="
                        + batch.baseSequence + " (" + batch.values.size() + " records, "
                        + config.deliveryTimeoutMs() + " ms)",
                last);
        for (var f : batch.futures) {
            f.completeExceptionally(failure);
        }
    }

    /**
     * Exact encoded size of one null-key, header-less record at this
     * offsetDelta — delegate to the storage layer's own sizing so the
     * accumulator's byte accounting matches the wire format.
     */
    private static int encodedRecordSize(int offsetDelta, byte[] value) {
        return RecordBatch.estimatedSize(List.of(new Record(offsetDelta, 0L, null, value)))
                - RecordBatch.BATCH_OVERHEAD;
    }

    private record TopicPartition(String topic, int partition) {}

    /**
     * Accumulation state for one partition. {@code nextSequence} is the
     * partition's monotonic sequence cursor: assigned to a batch at creation,
     * advanced per record, never reused — the broker's contiguity check
     * (next base sequence == previous base + previous count) depends on it.
     */
    private static final class PartitionState {
        final ArrayDeque<Batch> sealed = new ArrayDeque<>();
        Batch open;
        int nextSequence;
    }

    /**
     * One accumulating (then in-flight) batch. Mutated only under the
     * producer lock while it is the partition's {@code open} batch; once
     * sealed it is read-only and owned by the sender thread.
     */
    private static final class Batch {
        final String topic;
        final int partition;
        final int baseSequence;
        final long firstAppendNanos;
        final List<byte[]> values = new ArrayList<>();
        final List<CompletableFuture<Long>> futures = new ArrayList<>();
        int encodedBytes = RecordBatch.BATCH_OVERHEAD;

        Batch(String topic, int partition, int baseSequence, long firstAppendNanos) {
            this.topic = topic;
            this.partition = partition;
            this.baseSequence = baseSequence;
            this.firstAppendNanos = firstAppendNanos;
        }
    }
}
