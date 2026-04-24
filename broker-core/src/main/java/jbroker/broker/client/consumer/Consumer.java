package jbroker.broker.client.consumer;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
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
import java.util.concurrent.TimeUnit;
import jbroker.proto.broker.CommitOffsetsRequest;
import jbroker.proto.broker.ConsumerGroupHeartbeatRequest;
import jbroker.proto.broker.ConsumerGrpc;
import jbroker.proto.broker.FetchOffsetsRequest;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.broker.FindCoordinatorRequest;
import jbroker.proto.broker.OffsetCommit;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ProducerGrpc;
import jbroker.proto.broker.TopicPartitions;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import jbroker.tls.TlsContexts;

/**
 * Milestone 7 consumer client. Single-threaded — the application drives the
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
 *   <li>{@link #commitSync()} to persist the current offsets, or
 *       {@link #commitSync(Map)} to commit a custom set.</li>
 *   <li>{@link #close} to leave the group cleanly (sends
 *       {@code member_epoch=-1}) and release the gRPC channel.</li>
 * </ol>
 *
 * <p>Cooperative incremental rebalance () is honoured automatically:
 * the consumer reports {@code owned_partitions} on each heartbeat, the
 * coordinator advances stages, and the listener fires on the actual diffs.
 * Apps that don't care about the staged dance can pass
 * {@link RebalanceListener#NO_OP}.
 *
 * <p>Coordinator caching: the first successful {@link #findCoordinator}
 * call caches the endpoint. On {@code NOT_COORDINATOR}, the cache is
 * cleared and the next call refreshes via {@code FindCoordinator}.
 *
 * <p>Not Kafka-API-compatible — see {@code BrokerClient} for the producer
 * surface.
 */
public final class Consumer<K, V> implements AutoCloseable {

    private final ConsumerConfig cfg;
    private final Deserializer<K> keyDe;
    private final Deserializer<V> valueDe;
    private final ManagedChannel bootstrapChannel;
    private final ConsumerGrpc.ConsumerBlockingStub bootstrapStub;
    private ManagedChannel coordinatorChannel;
    private ConsumerGrpc.ConsumerBlockingStub coordinatorStub;
    private CoordinatorEndpoint cachedCoordinator;

    private Set<String> subscribed = Set.of();
    private RebalanceListener listener = RebalanceListener.NO_OP;
    private String memberId = "";
    private int memberEpoch = 0;
    private List<TopicPartition> currentAssignment = List.of();
    // per-(topic,partition) next fetch offset; populated lazily on first
    // assignment via FetchOffsets, then advanced as records arrive.
    private final Map<TopicPartition, Long> fetchOffsets = new HashMap<>();
    // Lazy ProducerGrpc stub over the bootstrap channel for DLT routing.
    private ProducerGrpc.ProducerBlockingStub dltProducerStub;
    // incremental fetch session. 0 = no session; any positive value
    // is echoed on subsequent Fetch requests so the broker can reuse cached
    // per-partition state. Reset to 0 on FETCH_SESSION_ID_NOT_FOUND (LRU
    // eviction or broker restart).
    private int fetchSessionId;
    private int fetchSessionEpoch;
    private boolean closed;

    public Consumer(ConsumerConfig cfg, Deserializer<K> keyDe, Deserializer<V> valueDe) {
        this.cfg = cfg;
        this.keyDe = keyDe;
        this.valueDe = valueDe;
        this.bootstrapChannel = buildChannel(cfg, cfg.bootstrapHost(), cfg.bootstrapPort());
        this.bootstrapStub = ConsumerGrpc.newBlockingStub(bootstrapChannel);
    }

    private static ManagedChannel buildChannel(ConsumerConfig cfg, String host, int port) {
        var b = NettyChannelBuilder.forAddress(host, port);
        try {
            var sslCtx = TlsContexts.clientContext(cfg.tls());
            if (sslCtx == null) {
                b.usePlaintext();
            } else {
                b.sslContext(sslCtx);
            }
        } catch (javax.net.ssl.SSLException e) {
            throw new IllegalStateException("TLS client context build failed", e);
        }
        return b.build();
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
     * Drive the heartbeat + fetch state machine and return any new records
     * that arrived this tick. {@code maxWait} bounds the per-fetch gRPC
     * deadline (the broker's Fetch handler returns immediately when no new
     * records are available; this only matters under network slowness).
     */
    public synchronized ConsumerRecords<K, V> poll(Duration maxWait) {
        if (closed) throw new IllegalStateException("consumer is closed");
        if (subscribed.isEmpty()) return ConsumerRecords.empty();

        var stub = ensureCoordinator();
        if (stub == null) return ConsumerRecords.empty();

        var hbReq = ConsumerGroupHeartbeatRequest.newBuilder()
                .setGroupId(cfg.groupId())
                .setMemberId(memberId)
                .setMemberEpoch(memberEpoch)
                .setInstanceId(cfg.instanceId())
                .setRebalanceTimeoutMs(cfg.rebalanceTimeoutMs())
                .addAllSubscribedTopics(subscribed)
                .addAllOwnedPartitions(toProtoTopicPartitions(currentAssignment));
        var hbResp = stub.withDeadlineAfter(cfg.pollFetchDeadline().toMillis(), TimeUnit.MILLISECONDS)
                .consumerGroupHeartbeat(hbReq.build());

        if (hbResp.getError() == ErrorCode.NOT_COORDINATOR
                || hbResp.getError() == ErrorCode.COORDINATOR_NOT_AVAILABLE) {
            invalidateCoordinator();
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

        return fetchAssignedPartitions(stub);
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
     * per-partition fetch offsets — handled-successfully, handled-then-DLT'd,
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
        var resp = lazyDltProducer()
                .withDeadlineAfter(cfg.pollFetchDeadline().toMillis(), TimeUnit.MILLISECONDS)
                .produce(req);
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

    private ProducerGrpc.ProducerBlockingStub lazyDltProducer() {
        if (dltProducerStub == null) {
            dltProducerStub = ProducerGrpc.newBlockingStub(bootstrapChannel);
        }
        return dltProducerStub;
    }

    /** Commit the consumer's current per-partition fetch offsets. */
    public synchronized Map<TopicPartition, OffsetAndMetadata> commitSync() {
        var snapshot = new HashMap<TopicPartition, OffsetAndMetadata>(currentAssignment.size());
        for (var tp : currentAssignment) {
            var off = fetchOffsets.get(tp);
            if (off == null) continue;
            snapshot.put(tp, new OffsetAndMetadata(off));
        }
        if (snapshot.isEmpty()) return Map.of();
        commitSync(snapshot);
        return snapshot;
    }

    public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty()) return;
        var stub = ensureCoordinator();
        if (stub == null) throw new IllegalStateException("coordinator not available");
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
        var resp = stub.withDeadlineAfter(cfg.pollFetchDeadline().toMillis(), TimeUnit.MILLISECONDS)
                .commitOffsets(b.build());
        for (var r : resp.getResultsList()) {
            if (r.getError() == ErrorCode.NOT_COORDINATOR) {
                invalidateCoordinator();
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
        var stub = ensureCoordinator();
        if (stub == null) return new OffsetAndMetadata(-1L);
        var resp = stub.withDeadlineAfter(cfg.pollFetchDeadline().toMillis(), TimeUnit.MILLISECONDS)
                .fetchOffsets(FetchOffsetsRequest.newBuilder()
                        .setGroupId(cfg.groupId())
                        .addTps(tp)
                        .build());
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
        try {
            if (!memberId.isEmpty() && coordinatorStub != null) {
                coordinatorStub
                        .withDeadlineAfter(2, TimeUnit.SECONDS)
                        .consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest.newBuilder()
                                .setGroupId(cfg.groupId())
                                .setMemberId(memberId)
                                .setMemberEpoch(-1)
                                .build());
            }
        } catch (Exception ignored) {
            // best-effort
        }
        if (!currentAssignment.isEmpty()) {
            listener.onPartitionsRevoked(currentAssignment);
            currentAssignment = List.of();
        }
        shutdownChannel(coordinatorChannel);
        shutdownChannel(bootstrapChannel);
    }

    // ---------- internals ----------

    private record CoordinatorEndpoint(String host, int port) {}

    private ConsumerGrpc.ConsumerBlockingStub ensureCoordinator() {
        if (cachedCoordinator != null && coordinatorStub != null) {
            return coordinatorStub;
        }
        var resp = bootstrapStub
                .withDeadlineAfter(cfg.pollFetchDeadline().toMillis(), TimeUnit.MILLISECONDS)
                .findCoordinator(FindCoordinatorRequest.newBuilder()
                        .setKey(cfg.groupId())
                        .build());
        if (resp.getError() != ErrorCode.OK) {
            return null;
        }
        var ep = new CoordinatorEndpoint(
                resp.getCoordinator().getHost(), resp.getCoordinator().getPort());
        cachedCoordinator = ep;
        if (ep.host().equals(cfg.bootstrapHost()) && ep.port() == cfg.bootstrapPort()) {
            // Coordinator is the bootstrap broker — reuse the channel.
            coordinatorChannel = null;
            coordinatorStub = bootstrapStub;
        } else {
            coordinatorChannel = buildChannel(cfg, ep.host(), ep.port());
            coordinatorStub = ConsumerGrpc.newBlockingStub(coordinatorChannel);
        }
        return coordinatorStub;
    }

    private void invalidateCoordinator() {
        cachedCoordinator = null;
        if (coordinatorChannel != null) {
            shutdownChannel(coordinatorChannel);
            coordinatorChannel = null;
        }
        coordinatorStub = null;
    }

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
            for (var tp : revoked) fetchOffsets.remove(tp);
        }
        currentAssignment = List.copyOf(newAssignment);
        if (!added.isEmpty()) {
            listener.onPartitionsAssigned(added);
            // Prime fetch offsets for newly-added partitions from the
            // coordinator's committed view (or 0 if never committed).
            for (var tp : added) {
                var committed = committedQuiet(tp);
                fetchOffsets.put(tp, committed.offset() < 0 ? 0L : committed.offset());
            }
        }
    }

    private OffsetAndMetadata committedQuiet(TopicPartition tp) {
        try {
            return committed(tp);
        } catch (Exception e) {
            return new OffsetAndMetadata(-1L);
        }
    }

    private ConsumerRecords<K, V> fetchAssignedPartitions(ConsumerGrpc.ConsumerBlockingStub stub) {
        var perTp = new LinkedHashMap<TopicPartition, List<ConsumerRecord<K, V>>>();
        for (var tp : currentAssignment) {
            long offset = fetchOffsets.getOrDefault(tp, 0L);
            var resp = stub.withDeadlineAfter(cfg.pollFetchDeadline().toMillis(), TimeUnit.MILLISECONDS)
                    .fetch(FetchRequest.newBuilder()
                            .setTopic(tp.getTopic())
                            .setPartition(tp.getPartition())
                            .setOffset(offset)
                            .setMaxBytes(cfg.fetchMaxBytes())
                            .setSessionId(fetchSessionId)
                            .setSessionEpoch(fetchSessionEpoch)
                            .build());
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
            var records = decodeBatch(resp.getRecords(), tp, offset);
            if (!records.isEmpty()) {
                perTp.put(tp, records);
                long lastOffset = records.get(records.size() - 1).offset();
                fetchOffsets.put(tp, lastOffset + 1);
            }
        }
        return new ConsumerRecords<>(perTp);
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

    private static void shutdownChannel(ManagedChannel ch) {
        if (ch == null) return;
        ch.shutdown();
        try {
            ch.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
