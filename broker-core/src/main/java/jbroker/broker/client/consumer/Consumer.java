package jbroker.broker.client.consumer;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import jbroker.broker.client.ClusterClient;
import jbroker.proto.broker.CommitOffsetsRequest;
import jbroker.proto.broker.ConsumerGroupHeartbeatRequest;
import jbroker.proto.broker.FetchOffsetsRequest;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.broker.ListOffsetsPartition;
import jbroker.proto.broker.ListOffsetsRequest;
import jbroker.proto.broker.OffsetCommit;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.TopicPartitions;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;

/**
 * Consumer client. Single-threaded — the application drives the
 * state machine by repeatedly calling {@link #poll}, which handles
 * heartbeats, fetches, and rebalance callbacks inline.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Construct with a {@link ConsumerConfig} + value/key
 *       {@link Deserializer}s.</li>
 *   <li>{@link #subscribe} to a set of topics with an optional
 *       {@link RebalanceListener}.</li>
 *   <li>Loop on {@link #poll}, processing returned records. Each poll
 *       sends one heartbeat (joining on first call) and one fetch per
 *       assigned partition, and fires
 *       {@link RebalanceListener#onPartitionsRevoked} /
 *       {@link RebalanceListener#onPartitionsAssigned} when the
 *       coordinator hands back a changed assignment.</li>
 *   <li>{@link #commitSync()} to persist the current offsets,
 *       {@link #commitSync(Map)} to commit a custom set, or the
 *       {@link #commitAsync()} variants to commit off the poll thread.</li>
 *   <li>{@link #close} to leave the group cleanly (sends
 *       {@code member_epoch=-1}) and release the gRPC channel.</li>
 * </ol>
 *
 * <p>Flow control: {@link #pause}/{@link #resume} stop and restart
 * fetching per partition without touching group membership (heartbeats
 * keep flowing), and {@code max.poll.records} bounds how many records a
 * single poll hands back — surplus already fetched waits client-side for
 * the next poll. {@link #seek} repositions the fetch cursor;
 * {@link #seekToBeginning}/{@link #seekToEnd} resolve the log's actual
 * bounds from the broker via {@code ListOffsets} first.
 *
 * <p>Cooperative incremental rebalance is honoured automatically:
 * the consumer reports {@code owned_partitions} on each heartbeat, the
 * coordinator advances stages, and the listener fires on the actual diffs.
 * Apps that don't care about the staged dance can pass
 * {@link RebalanceListener#NO_OP}.
 *
 * <p>Coordinator caching: the first successful {@code FindCoordinator}
 * call caches the endpoint. On {@code NOT_COORDINATOR}, the cache is
 * cleared and the next call refreshes via {@code FindCoordinator}.
 *
 * <p>Network wiring is behind the {@link ConsumerRpc} seam: the classic
 * constructor keeps the original single-endpoint behavior (bootstrap
 * broker + cached coordinator channel), while the {@link ClusterClient}
 * constructor routes every call cluster-wide so poll/commit loops ride
 * through leader and coordinator failover with no application-side error
 * handling.
 *
 * <p>Not Kafka-API-compatible — see {@code BrokerClient} for the producer
 * surface.
 */
public final class Consumer<K, V> implements AutoCloseable {

    // ListOffsets timestamp sentinels (see broker.proto): -1 = latest,
    // -2 = earliest.
    private static final long LATEST_TIMESTAMP = -1L;
    private static final long EARLIEST_TIMESTAMP = -2L;

    private final ConsumerConfig cfg;
    private final Deserializer<K> keyDe;
    private final Deserializer<V> valueDe;
    private final ConsumerRpc rpc;

    private Set<String> subscribed = Set.of();
    private RebalanceListener listener = RebalanceListener.NO_OP;
    private String memberId = "";
    private int memberEpoch = 0;
    private List<TopicPartition> currentAssignment = List.of();
    // Per-partition positions, pause flags, and the fetched-but-unreturned
    // buffer. Positions are populated lazily on first assignment via
    // FetchOffsets, then advanced as records are returned from poll.
    private final FetchState<K, V> fetchState = new FetchState<>();
    // Lazy single-thread executor behind commitAsync. One thread keeps
    // overlapping async commits in submission order.
    private ExecutorService commitExecutor;
    // Incremental fetch session. 0 = no session; any positive value
    // is echoed on subsequent Fetch requests so the broker can reuse cached
    // per-partition state. Reset to 0 on FETCH_SESSION_ID_NOT_FOUND (LRU
    // eviction, broker restart, or a cluster-routed fetch landing on a
    // different broker after failover).
    private int fetchSessionId;
    private int fetchSessionEpoch;
    private boolean closed;

    public Consumer(ConsumerConfig cfg, Deserializer<K> keyDe, Deserializer<V> valueDe) {
        this(cfg, keyDe, valueDe, new SingleEndpointConsumerRpc(cfg));
    }

    /**
     * Cluster-aware construction path: fetch, commit, and coordinator
     * calls route through {@code cluster}, which the application owns
     * (and may share with a producer). {@code cfg}'s bootstrap host/port
     * are ignored — discovery comes from the ClusterClient.
     */
    public Consumer(ConsumerConfig cfg, Deserializer<K> keyDe, Deserializer<V> valueDe, ClusterClient cluster) {
        this(cfg, keyDe, valueDe, new ClusterConsumerRpc(cluster, cfg));
    }

    Consumer(ConsumerConfig cfg, Deserializer<K> keyDe, Deserializer<V> valueDe, ConsumerRpc rpc) {
        this.cfg = cfg;
        this.keyDe = keyDe;
        this.valueDe = valueDe;
        this.rpc = rpc;
    }

    public synchronized void subscribe(Collection<String> topics, RebalanceListener listener) {
        if (closed) throw new IllegalStateException("consumer is closed");
        this.subscribed = Set.copyOf(topics);
        this.listener = listener == null ? RebalanceListener.NO_OP : listener;
        // Force a fresh join on next poll — coordinator allocates new
        // member_id and runs the assignor against this subscription.
        this.memberId = "";
        this.memberEpoch = 0;
    }

    public synchronized Set<TopicPartition> assignment() {
        return Set.copyOf(currentAssignment);
    }

    /**
     * Reposition the fetch cursor of an assigned partition. The next poll
     * fetches from {@code offset}; records already fetched from the old
     * position are discarded, never returned. Pause state survives a seek.
     */
    public synchronized void seek(String topic, int partition, long offset) {
        if (offset < 0) throw new IllegalArgumentException("offset must be >= 0: " + offset);
        fetchState.seek(assignedTp(topic, partition), offset);
    }

    /** Seek to the log's start offset, as the broker reports it via {@code ListOffsets}. */
    public synchronized void seekToBeginning(String topic, int partition) {
        var tp = assignedTp(topic, partition);
        fetchState.seek(tp, resolveOffset(tp, EARLIEST_TIMESTAMP));
    }

    /** Seek to the log's end offset — the offset the next produced record will take. */
    public synchronized void seekToEnd(String topic, int partition) {
        var tp = assignedTp(topic, partition);
        fetchState.seek(tp, resolveOffset(tp, LATEST_TIMESTAMP));
    }

    /**
     * Stop fetching from an assigned partition until {@link #resume}. Polls
     * keep heartbeating and the partition stays owned — only the fetch is
     * skipped, and any already-buffered records are held back until resume.
     */
    public synchronized void pause(String topic, int partition) {
        fetchState.pause(assignedTp(topic, partition));
    }

    /** Undo {@link #pause}; the next poll fetches the partition again. */
    public synchronized void resume(String topic, int partition) {
        fetchState.resume(assignedTp(topic, partition));
    }

    /** Partitions currently held back by {@link #pause}. */
    public synchronized Set<TopicPartition> paused() {
        return fetchState.paused();
    }

    /**
     * Drive the heartbeat + fetch state machine and return any new records
     * that arrived this tick. {@code maxWait} bounds the per-fetch gRPC
     * deadline (the broker's Fetch handler returns immediately when no new
     * records are available; this only matters under network slowness).
     */
    public synchronized ConsumerRecords<K, V> poll(Duration maxWait) {
        if (closed) throw new IllegalStateException("consumer is closed");
        if (subscribed.isEmpty()) return ConsumerRecords.empty();

        var hbReq = ConsumerGroupHeartbeatRequest.newBuilder()
                .setGroupId(cfg.groupId())
                .setMemberId(memberId)
                .setMemberEpoch(memberEpoch)
                .setInstanceId(cfg.instanceId())
                .setRebalanceTimeoutMs(cfg.rebalanceTimeoutMs())
                .addAllSubscribedTopics(subscribed)
                .addAllOwnedPartitions(toProtoTopicPartitions(currentAssignment));
        var hbResp = rpc.heartbeat(hbReq.build());
        if (hbResp == null) return ConsumerRecords.empty(); // no coordinator this tick

        if (hbResp.getError() == ErrorCode.NOT_COORDINATOR
                || hbResp.getError() == ErrorCode.COORDINATOR_NOT_AVAILABLE) {
            rpc.invalidateCoordinator();
            return ConsumerRecords.empty();
        }
        if (hbResp.getError() == ErrorCode.UNKNOWN_MEMBER_ID || hbResp.getError() == ErrorCode.FENCED_MEMBER_EPOCH) {
            // Force a fresh join on the next poll.
            memberId = "";
            memberEpoch = 0;
            currentAssignment = List.of();
            return ConsumerRecords.empty();
        }
        if (hbResp.getError() != ErrorCode.OK) {
            throw new RuntimeException("heartbeat failed: " + hbResp.getError());
        }
        memberId = hbResp.getMemberId();
        memberEpoch = hbResp.getMemberEpoch();
        if (hbResp.hasAssignment() && hbResp.getAssignment().getAssignedPartitionsCount() > 0) {
            applyAssignment(flatten(hbResp.getAssignment()));
        } else if (memberId.isEmpty() == false && hbResp.getAssignment().getAssignedPartitionsCount() == 0) {
            // Steady state — currentAssignment unchanged. (Empty Assignment in
            // a steady-state response means "no change," not "you have no
            // partitions" — that's covered by the join path above.)
        }

        if (currentAssignment.isEmpty()) return ConsumerRecords.empty();

        return fetchAssignedPartitions();
    }

    /**
     * Poll variant that runs {@code handler} on each fetched record inline.
     * On {@link RetryableException} the record is retried up to the configured
     * {@link DeadLetterPolicy#maxAttempts()} (sleeping {@code backoff} between
     * attempts); on final failure it is produced to the DLT carrying an
     * {@code X-DLT-Failure-Cause} header, and the consumer's committed offset
     * advances past it. If no {@link DeadLetterPolicy} is configured the
     * final {@link RetryableException} is wrapped and thrown.
     *
     * <p>After the tick completes, the consumer auto-commits the resulting
     * per-partition positions — handled-successfully, handled-then-DLT'd,
     * and anything pre-existing — via one synchronous {@code CommitOffsets}.
     */
    public synchronized ConsumerRecords<K, V> poll(Duration maxWait, RecordHandler<K, V> handler) {
        if (handler == null) throw new IllegalArgumentException("handler must not be null");
        var records = poll(maxWait);
        if (records.isEmpty()) return records;
        var policy = cfg.deadLetterPolicy();
        for (var rec : records) {
            runWithRetry(rec, handler, policy);
        }
        commitSync();
        return records;
    }

    private void runWithRetry(ConsumerRecord<K, V> rec, RecordHandler<K, V> handler, DeadLetterPolicy policy) {
        int maxAttempts = policy == null ? 1 : policy.maxAttempts();
        RetryableException lastCause = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                handler.handle(rec);
                return;
            } catch (RetryableException e) {
                lastCause = e;
                if (attempt < maxAttempts && policy != null && !policy.backoff().isZero()) {
                    try {
                        Thread.sleep(policy.backoff().toMillis());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("interrupted during DLT backoff", ie);
                    }
                }
            }
        }
        if (policy == null) {
            throw new RuntimeException(
                    "record handler rejected " + rec.tp().getTopic() + "-"
                            + rec.tp().getPartition() + "@" + rec.offset() + " and no DeadLetterPolicy is configured",
                    lastCause);
        }
        produceToDlt(rec, lastCause, policy);
    }

    private void produceToDlt(ConsumerRecord<K, V> rec, RetryableException cause, DeadLetterPolicy policy) {
        // Reuse the original record's raw headers; append X-DLT-Failure-Cause.
        byte[][] src = rec.headers();
        byte[][] decorated = new byte[src.length + 2][];
        System.arraycopy(src, 0, decorated, 0, src.length);
        decorated[src.length] = "X-DLT-Failure-Cause".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String causeText = cause.getClass().getName() + ": " + (cause.getMessage() == null ? "" : cause.getMessage());
        decorated[src.length + 1] = causeText.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        byte[] keyBytes = rec.key() == null ? null : serializeForDlt(rec.key());
        byte[] valueBytes = rec.value() == null ? null : serializeForDlt(rec.value());
        var record = new Record(0, 0L, keyBytes, valueBytes, decorated);
        var records = List.of(record);
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        long now = System.currentTimeMillis();
        RecordBatch.encode(buf, 0L, 0, now, now, -1L, (short) -1, -1, records);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);

        var req = ProduceRequest.newBuilder()
                .setTopic(policy.dltTopic())
                .setPartition(rec.tp().getPartition())
                .setBatch(ByteString.copyFrom(bytes))
                .setProducerId(-1L)
                .setBaseSequence(-1)
                .setAcks(1)
                .build();
        var resp = rpc.produceDlt(req);
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException("DLT produce to " + policy.dltTopic() + "-"
                    + rec.tp().getPartition() + " failed: " + resp.getError().getMessage());
        }
    }

    /**
     * DLT produce needs the original bytes. For {@code byte[]} keys/values the
     * deserialized form IS the wire form. For any other type the plain
     * {@code toString().getBytes(UTF_8)} is the cheapest round-trip; callers
     * that need exact-bytes fidelity should use {@code ByteArrayDeserializer}.
     */
    @SuppressWarnings("unchecked")
    private static byte[] serializeForDlt(Object o) {
        if (o instanceof byte[] b) return b;
        if (o instanceof String s) return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return o.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /** Commit the consumer's current per-partition positions. */
    public synchronized Map<TopicPartition, OffsetAndMetadata> commitSync() {
        var snapshot = positionSnapshot();
        if (snapshot.isEmpty()) return Map.of();
        commitSync(snapshot);
        return snapshot;
    }

    /**
     * Positions to commit: one entry per assigned partition that has been
     * primed. A position only ever covers records the application has seen —
     * buffered-but-unreturned records don't advance it (see {@link FetchState}).
     */
    private Map<TopicPartition, OffsetAndMetadata> positionSnapshot() {
        var snapshot = new HashMap<TopicPartition, OffsetAndMetadata>(currentAssignment.size());
        for (var tp : currentAssignment) {
            if (!fetchState.hasPosition(tp)) continue;
            snapshot.put(tp, new OffsetAndMetadata(fetchState.position(tp)));
        }
        return snapshot;
    }

    /**
     * Commit the consumer's current positions without blocking the caller.
     * The position snapshot is taken now, synchronously — records returned
     * by later polls can't leak into it — and the commit itself runs on a
     * single background thread, so overlapping {@code commitAsync} calls
     * reach the coordinator in submission order. The future completes when
     * the coordinator acks, or exceptionally on any failure — a failed
     * commit is never swallowed. Completes immediately when there is
     * nothing to commit.
     */
    public synchronized CompletableFuture<Void> commitAsync() {
        if (closed) throw new IllegalStateException("consumer is closed");
        var snapshot = positionSnapshot();
        if (snapshot.isEmpty()) return CompletableFuture.completedFuture(null);
        return commitAsync(snapshot);
    }

    /** {@link #commitAsync()} for an explicit offset map. */
    public synchronized CompletableFuture<Void> commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (closed) throw new IllegalStateException("consumer is closed");
        if (offsets.isEmpty()) return CompletableFuture.completedFuture(null);
        var copy = Map.copyOf(offsets);
        var future = new CompletableFuture<Void>();
        lazyCommitExecutor().execute(() -> {
            try {
                commitSync(copy);
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    private ExecutorService lazyCommitExecutor() {
        if (commitExecutor == null) {
            commitExecutor = Executors.newSingleThreadExecutor(r -> {
                var t = new Thread(r, "consumer-commit-" + cfg.groupId());
                t.setDaemon(true);
                return t;
            });
        }
        return commitExecutor;
    }

    public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty()) return;
        var b = CommitOffsetsRequest.newBuilder()
                .setGroupId(cfg.groupId())
                .setMemberId(memberId)
                .setGenerationIdOrMemberEpoch(memberEpoch);
        for (var e : offsets.entrySet()) {
            b.addCommits(OffsetCommit.newBuilder()
                    .setTp(e.getKey())
                    .setOffset(e.getValue().offset())
                    .setLeaderEpoch(e.getValue().leaderEpoch())
                    .setMetadata(e.getValue().metadata())
                    .build());
        }
        var resp = rpc.commitOffsets(b.build());
        if (resp == null) throw new IllegalStateException("coordinator not available");
        for (var r : resp.getResultsList()) {
            if (r.getError() == ErrorCode.NOT_COORDINATOR) {
                rpc.invalidateCoordinator();
                throw new RuntimeException("coordinator moved; retry commit");
            }
            if (r.getError() != ErrorCode.OK) {
                throw new RuntimeException("commit for " + r.getTp().getTopic() + "-"
                        + r.getTp().getPartition() + " failed: " + r.getError());
            }
        }
    }

    /** Look up the last committed offset for {@code tp} via the coordinator. */
    public synchronized OffsetAndMetadata committed(TopicPartition tp) {
        var resp = rpc.fetchOffsets(FetchOffsetsRequest.newBuilder()
                .setGroupId(cfg.groupId())
                .addTps(tp)
                .build());
        if (resp == null) return new OffsetAndMetadata(-1L);
        var r = resp.getResults(0);
        if (r.getError() == ErrorCode.OFFSET_OUT_OF_RANGE) {
            return new OffsetAndMetadata(-1L);
        }
        if (r.getError() != ErrorCode.OK) {
            throw new RuntimeException("fetchOffsets failed: " + r.getError());
        }
        return new OffsetAndMetadata(r.getOffset(), r.getLeaderEpoch(), r.getMetadata());
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        // Best-effort leave — if the coordinator is unreachable, the
        // session-timeout eviction will pick up the slack.
        if (!memberId.isEmpty()) {
            rpc.leaveGroup(ConsumerGroupHeartbeatRequest.newBuilder()
                    .setGroupId(cfg.groupId())
                    .setMemberId(memberId)
                    .setMemberEpoch(-1)
                    .build());
        }
        if (!currentAssignment.isEmpty()) {
            listener.onPartitionsRevoked(currentAssignment);
            currentAssignment = List.of();
        }
        rpc.close();
        // Async commits still queued now run against the closed transport
        // and complete exceptionally — loud, never dropped.
        if (commitExecutor != null) commitExecutor.shutdown();
    }

    // ---------- internals ----------

    private void applyAssignment(List<TopicPartition> newAssignment) {
        var oldSet = new HashSet<>(currentAssignment);
        var newSet = new HashSet<>(newAssignment);
        var revoked = new ArrayList<TopicPartition>();
        for (var tp : currentAssignment) {
            if (!newSet.contains(tp)) revoked.add(tp);
        }
        var added = new ArrayList<TopicPartition>();
        for (var tp : newAssignment) {
            if (!oldSet.contains(tp)) added.add(tp);
        }
        if (!revoked.isEmpty()) {
            listener.onPartitionsRevoked(revoked);
            for (var tp : revoked) fetchState.forget(tp);
        }
        currentAssignment = List.copyOf(newAssignment);
        if (!added.isEmpty()) {
            listener.onPartitionsAssigned(added);
            // Prime positions for newly-added partitions from the
            // coordinator's committed view (or 0 if never committed) — but
            // never rewind a live local position. A rejoin after member
            // eviction (UNKNOWN_MEMBER_ID) re-adds partitions this
            // instance never stopped owning; re-priming below the local
            // position would re-deliver records the application already
            // saw. Committed-ahead still wins: another member may have
            // advanced the group while we were out.
            for (var tp : added) {
                var committed = committedQuiet(tp);
                long floor = committed.offset() < 0 ? 0L : committed.offset();
                long resume = fetchState.hasPosition(tp) ? Math.max(fetchState.position(tp), floor) : floor;
                fetchState.position(tp, resume);
            }
        }
    }

    /**
     * Ask the broker serving our fetches where the log starts or ends.
     * Same {@code ListOffsets} sentinel convention as the wire protocol:
     * {@code -2} earliest, {@code -1} latest.
     */
    private long resolveOffset(TopicPartition tp, long timestamp) {
        var resp = rpc.listOffsets(ListOffsetsRequest.newBuilder()
                .setReplicaId(-1)
                .addPartitions(ListOffsetsPartition.newBuilder()
                        .setTp(tp)
                        .setTimestamp(timestamp)
                        .build())
                .build());
        if (resp == null) throw new IllegalStateException("coordinator not available");
        var r = resp.getResults(0);
        if (r.getError() != ErrorCode.OK) {
            throw new RuntimeException(
                    "listOffsets for " + tp.getTopic() + "-" + tp.getPartition() + " failed: " + r.getError());
        }
        return r.getOffset();
    }

    private TopicPartition assignedTp(String topic, int partition) {
        var tp = TopicPartition.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .build();
        if (!currentAssignment.contains(tp)) {
            throw new IllegalStateException("no current assignment for " + topic + "-" + partition);
        }
        return tp;
    }

    private OffsetAndMetadata committedQuiet(TopicPartition tp) {
        try {
            return committed(tp);
        } catch (Exception e) {
            return new OffsetAndMetadata(-1L);
        }
    }

    private ConsumerRecords<K, V> fetchAssignedPartitions() {
        for (var tp : currentAssignment) {
            // Paused partitions and partitions still carrying surplus from a
            // previous poll issue no fetch this tick.
            if (!fetchState.fetchable(tp)) continue;
            long offset = fetchState.position(tp);
            var resp = rpc.fetch(FetchRequest.newBuilder()
                    .setTopic(tp.getTopic())
                    .setPartition(tp.getPartition())
                    .setOffset(offset)
                    .setMaxBytes(cfg.fetchMaxBytes())
                    .setSessionId(fetchSessionId)
                    .setSessionEpoch(fetchSessionEpoch)
                    .build());
            if (resp == null) continue; // partition unreachable this tick — try next poll
            if (resp.hasError() && resp.getError().getCode() != 0) {
                // Broker evicted (or never knew) our session — fall back to
                // a fresh bootstrap request on the next call.
                if (resp.getError().getCode() == ErrorCode.FETCH_SESSION_ID_NOT_FOUND.getNumber()) {
                    fetchSessionId = 0;
                    fetchSessionEpoch = 0;
                }
                continue; // skip this partition this tick — try next poll
            }
            // Echo the broker-assigned session id on subsequent requests;
            // bump epoch monotonically so the broker can order in-flight
            // (single-threaded client, so always trivially ordered).
            if (resp.getSessionId() != 0) {
                if (fetchSessionId == 0) {
                    fetchSessionId = resp.getSessionId();
                }
                fetchSessionEpoch++;
            }
            fetchState.buffer(tp, decodeBatch(resp.getRecords(), tp, offset));
        }
        // Hand back at most max.poll.records; anything fetched beyond the
        // bound stays buffered (positions advance only for returned records).
        return new ConsumerRecords<>(fetchState.drain(cfg.maxPollRecords()));
    }

    private List<ConsumerRecord<K, V>> decodeBatch(ByteString bytes, TopicPartition tp, long startOffset) {
        var buf = ByteBuffer.wrap(bytes.toByteArray());
        var out = new ArrayList<ConsumerRecord<K, V>>();
        while (buf.remaining() >= RecordBatch.BATCH_OVERHEAD) {
            int mark = buf.position();
            try {
                var parsed = RecordBatch.decode(buf);
                for (var rec : parsed.records()) {
                    long absoluteOffset = parsed.baseOffset() + rec.offsetDelta();
                    if (absoluteOffset < startOffset) continue; // pre-fetch-offset records
                    K key = rec.key() == null ? null : keyDe.deserialize(rec.key());
                    V value = rec.value() == null ? null : valueDe.deserialize(rec.value());
                    out.add(new ConsumerRecord<>(tp, absoluteOffset, key, value, rec.headers()));
                }
            } catch (IllegalArgumentException e) {
                buf.position(mark);
                break; // truncated trailing batch
            }
        }
        return out;
    }

    private static List<TopicPartitions> toProtoTopicPartitions(List<TopicPartition> tps) {
        var byTopic = new LinkedHashMap<String, List<Integer>>();
        for (var tp : tps) {
            byTopic.computeIfAbsent(tp.getTopic(), k -> new ArrayList<>()).add(tp.getPartition());
        }
        var out = new ArrayList<TopicPartitions>();
        for (var e : byTopic.entrySet()) {
            out.add(TopicPartitions.newBuilder()
                    .setTopic(e.getKey())
                    .addAllPartitions(e.getValue())
                    .build());
        }
        return out;
    }

    private static List<TopicPartition> flatten(jbroker.proto.broker.Assignment a) {
        var out = new ArrayList<TopicPartition>();
        for (var tp : a.getAssignedPartitionsList()) {
            for (int p : tp.getPartitionsList()) {
                out.add(TopicPartition.newBuilder()
                        .setTopic(tp.getTopic())
                        .setPartition(p)
                        .build());
            }
        }
        return out;
    }
}
