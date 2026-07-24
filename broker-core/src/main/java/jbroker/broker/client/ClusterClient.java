package jbroker.broker.client;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.random.RandomGenerator;
import jbroker.broker.ErrorCodeNames;
import jbroker.broker.ErrorCodes;
import jbroker.proto.broker.AdminGrpc;
import jbroker.proto.broker.CommitOffsetsRequest;
import jbroker.proto.broker.CommitOffsetsResponse;
import jbroker.proto.broker.ConsumerGroupHeartbeatRequest;
import jbroker.proto.broker.ConsumerGroupHeartbeatResponse;
import jbroker.proto.broker.ConsumerGrpc;
import jbroker.proto.broker.CreateTopicRequest;
import jbroker.proto.broker.CreateTopicResponse;
import jbroker.proto.broker.DescribeClusterRequest;
import jbroker.proto.broker.DescribeClusterResponse;
import jbroker.proto.broker.DescribeTopicPartitionsRequest;
import jbroker.proto.broker.DescribeTopicPartitionsResponse;
import jbroker.proto.broker.FetchOffsetsRequest;
import jbroker.proto.broker.FetchOffsetsResponse;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.broker.FetchResponse;
import jbroker.proto.broker.FindCoordinatorRequest;
import jbroker.proto.broker.FindCoordinatorResponse;
import jbroker.proto.broker.InitProducerIdRequest;
import jbroker.proto.broker.InitProducerIdResponse;
import jbroker.proto.broker.ListOffsetsRequest;
import jbroker.proto.broker.ListOffsetsResponse;
import jbroker.proto.broker.MetadataGrpc;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ProduceResponse;
import jbroker.proto.broker.ProducerGrpc;
import jbroker.proto.common.ErrorCode;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import jbroker.tls.TlsConfig;
import jbroker.tls.TlsContexts;

/**
 * Cluster-aware client core: owns endpoint discovery and request routing
 * so that produce, fetch, and coordinator traffic rides through leader
 * failover without the application ever seeing an error.
 *
 * <p>Bootstrap: constructed from a list of {@code host:port} endpoints;
 * the first {@code DescribeCluster} that answers seeds the cluster view
 * (broker id → advertised endpoint, controller id, liveness). Per-topic
 * partition leaders come from {@code DescribeTopicPartitions} and are
 * cached until proven stale.
 *
 * <p>Routing: leader-owned calls (produce, fetch, list-offsets) go to the
 * cached partition leader; controller-owned calls (init-producer-id, topic
 * creation) go to the cached Raft controller; group calls go to the cached
 * coordinator from {@code FindCoordinator}. On {@code NOT_LEADER} the
 * {@code suggested_leader_*} hints — when the broker filled them — update
 * the cache and the retry short-circuits straight to the hinted broker;
 * without hints the cache entry is invalidated and refreshed from live
 * metadata. On transport failure ({@code UNAVAILABLE},
 * {@code DEADLINE_EXCEEDED}) the failed channel is dropped and metadata is
 * refreshed through a different broker before the retry.
 *
 * <p>Retry policy: bounded exponential backoff with equal jitter —
 * attempt {@code k} sleeps a uniform value in
 * {@code [raw/2, raw]} where {@code raw = min(cap, base * mult^k)} —
 * until the per-call deadline; exhaustion raises
 * {@link RetriesExhaustedException} carrying the last failure. Which
 * broker errors retry follows {@link ErrorCodes}: NOT_LEADER,
 * NOT_ENOUGH_REPLICAS, STORAGE_FULL, QUOTA_VIOLATED, and the coordinator
 * relocation codes are retried; everything else (UNKNOWN_TOPIC,
 * MESSAGE_TOO_LARGE, UNAUTHORIZED, ...) fails fast because a retry of the
 * same request can never succeed.
 *
 * <p>Channel lifecycle: one lazily-connected channel per broker endpoint,
 * cached; a broker that disappears from the cluster view (and is not a
 * bootstrap endpoint) has its channel closed, as does every channel on
 * {@link #close()}.
 *
 * <p>Internally composes raw per-broker stubs rather than
 * {@link BrokerClient}: routing decisions need the error envelope's
 * numeric code and hint map, which BrokerClient collapses into a plain
 * RuntimeException message. BrokerClient stays as the single-endpoint
 * convenience client.
 */
public final class ClusterClient implements AutoCloseable {

    /** {@code host:port} dial target. */
    public record Endpoint(String host, int port) {
        public static Endpoint parse(String hostPort) {
            int colon = hostPort.lastIndexOf(':');
            if (colon <= 0 || colon == hostPort.length() - 1) {
                throw new IllegalArgumentException("expected host:port, got: " + hostPort);
            }
            return new Endpoint(hostPort.substring(0, colon), Integer.parseInt(hostPort.substring(colon + 1)));
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }
    }

    /**
     * Retry tuning. {@link #defaults()} gives 100 ms base backoff doubling
     * to a 1 s cap, a 120 s overall per-call deadline, and a 10 s
     * per-attempt RPC timeout (a hair above the broker's 5 s internal
     * acks=all and propose windows so server-side errors surface as
     * envelopes, not DEADLINE_EXCEEDED).
     */
    public record Config(
            long backoffBaseMs, double backoffMultiplier, long backoffCapMs, long callDeadlineMs, long rpcTimeoutMs) {

        public Config {
            if (backoffBaseMs < 1) throw new IllegalArgumentException("backoffBaseMs must be positive");
            if (backoffMultiplier < 1.0) throw new IllegalArgumentException("backoffMultiplier must be >= 1");
            if (backoffCapMs < backoffBaseMs) throw new IllegalArgumentException("backoffCapMs must be >= base");
            if (callDeadlineMs < 1) throw new IllegalArgumentException("callDeadlineMs must be positive");
            if (rpcTimeoutMs < 1) throw new IllegalArgumentException("rpcTimeoutMs must be positive");
        }

        public static Config defaults() {
            return new Config(100, 2.0, 1_000, 120_000, 10_000);
        }
    }

    /** A routed call ran out of per-call deadline; the cause is the last failure seen. */
    public static final class RetriesExhaustedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        RetriesExhaustedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Per-endpoint wire seam. Production code connects real gRPC channels
     * via the default factory; unit tests substitute a scripted fake so the
     * routing, cache-refresh, and backoff logic runs without a broker.
     */
    public interface Transport extends AutoCloseable {
        DescribeClusterResponse describeCluster(long timeoutMs);

        DescribeTopicPartitionsResponse describeTopicPartitions(String topic, long timeoutMs);

        InitProducerIdResponse initProducerId(long timeoutMs);

        ProduceResponse produce(ProduceRequest req, long timeoutMs);

        FetchResponse fetch(FetchRequest req, long timeoutMs);

        ListOffsetsResponse listOffsets(ListOffsetsRequest req, long timeoutMs);

        FindCoordinatorResponse findCoordinator(FindCoordinatorRequest req, long timeoutMs);

        ConsumerGroupHeartbeatResponse consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest req, long timeoutMs);

        CommitOffsetsResponse commitOffsets(CommitOffsetsRequest req, long timeoutMs);

        FetchOffsetsResponse fetchOffsets(FetchOffsetsRequest req, long timeoutMs);

        CreateTopicResponse createTopic(CreateTopicRequest req, long timeoutMs);

        @Override
        void close();
    }

    /** Connects a {@link Transport} to one endpoint. */
    public interface TransportFactory {
        Transport connect(Endpoint endpoint);
    }

    /** Time + sleep seam so unit tests can drive the deadline clock. */
    interface Ticker {
        long nanoTime();

        void sleep(long millis) throws InterruptedException;

        Ticker SYSTEM = new Ticker() {
            @Override
            public long nanoTime() {
                return System.nanoTime();
            }

            @Override
            public void sleep(long millis) throws InterruptedException {
                Thread.sleep(millis);
            }
        };
    }

    private record Tp(String topic, int partition) {
        @Override
        public String toString() {
            return topic + "-" + partition;
        }
    }

    /** Leader + ISR snapshot for one partition, as last described. */
    private record PartitionInfo(int leader, List<Integer> isr) {}

    /** Immutable broker-level view from the last successful DescribeCluster. */
    private record ClusterView(Map<Integer, Endpoint> brokers, Map<Integer, Boolean> alive, int controllerId) {
        static final ClusterView EMPTY = new ClusterView(Map.of(), Map.of(), -1);
    }

    /** Metadata RPC timeout: fail over to the next candidate quickly. */
    private static final long METADATA_TIMEOUT_MS = 5_000;

    private final List<Endpoint> bootstrap;
    private final Config config;
    private final TransportFactory factory;
    private final Ticker ticker;
    private final RandomGenerator random;

    private final Map<Endpoint, Transport> transports = new ConcurrentHashMap<>();
    private final Map<Tp, PartitionInfo> leaders = new ConcurrentHashMap<>();
    private final Map<String, Endpoint> coordinators = new ConcurrentHashMap<>();
    private volatile ClusterView view = ClusterView.EMPTY;
    private final Object refreshLock = new Object();
    private volatile boolean closed;

    public ClusterClient(List<String> bootstrapEndpoints) {
        this(bootstrapEndpoints, Config.defaults());
    }

    public ClusterClient(List<String> bootstrapEndpoints, Config config) {
        this(bootstrapEndpoints, config, TlsConfig.DISABLED);
    }

    public ClusterClient(List<String> bootstrapEndpoints, Config config, TlsConfig tls) {
        this(
                bootstrapEndpoints.stream().map(Endpoint::parse).toList(),
                config,
                grpcFactory(tls),
                Ticker.SYSTEM,
                new java.util.Random());
    }

    ClusterClient(
            List<Endpoint> bootstrap, Config config, TransportFactory factory, Ticker ticker, RandomGenerator random) {
        if (bootstrap.isEmpty()) throw new IllegalArgumentException("bootstrap endpoints must be non-empty");
        this.bootstrap = List.copyOf(bootstrap);
        this.config = Objects.requireNonNull(config, "config");
        this.factory = Objects.requireNonNull(factory, "factory");
        this.ticker = Objects.requireNonNull(ticker, "ticker");
        this.random = Objects.requireNonNull(random, "random");
    }

    // ---- Producer path ----

    /**
     * Allocate an idempotent-producer id from the controller. Routed to the
     * cached Raft controller (allocation commits through the metadata log);
     * NOT_LEADER refreshes the view and retries against the new controller.
     */
    public long initProducerId() {
        long deadline = deadlineNanos(config.callDeadlineMs());
        RuntimeException last = null;
        for (int attempt = 0; ; attempt++) {
            checkDeadline(deadline, "initProducerId", last);
            var ep = controllerOrAnyEndpoint();
            if (ep == null) {
                last = new RuntimeException("no reachable broker for initProducerId");
                backoff(attempt, deadline, "initProducerId", last);
                continue;
            }
            InitProducerIdResponse resp;
            try {
                resp = transportFor(ep).initProducerId(config.rpcTimeoutMs());
            } catch (StatusRuntimeException e) {
                last = onTransportFailure(ep, e, "initProducerId");
                backoff(attempt, deadline, "initProducerId", last);
                continue;
            }
            if (!isError(resp.hasError(), resp.getError().getCode())) return resp.getProducerId();
            int code = resp.getError().getCode();
            last = envelopeFailure("initProducerId", resp.getError());
            if (code == ErrorCodes.NOT_LEADER) {
                // Controller moved. The InitProducerId path fills no
                // suggested_leader_* hints, so go straight to a refresh.
                refreshClusterView(ep);
                backoff(attempt, deadline, "initProducerId", last);
                continue;
            }
            if (retriableCode(code)) {
                backoff(attempt, deadline, "initProducerId", last);
                continue;
            }
            throw last;
        }
    }

    /**
     * Idempotent batched produce with acks=all, routed to the partition
     * leader and retried through failover. Same wire shape as
     * {@link BrokerClient#idempotentProduceBatchAcksAll}: one RecordBatch
     * carrying {@code (producerId, epoch, baseSequence)}, so a retry of the
     * SAME sequence dedupes broker-side instead of double-appending.
     * Returns the absolute offset of the last record.
     */
    public long produceIdempotentBatchAcksAll(
            String topic, int partition, List<byte[]> values, long producerId, int epoch, int baseSequence) {
        if (values.isEmpty()) throw new IllegalArgumentException("values must be non-empty");
        var records = new ArrayList<Record>(values.size());
        for (int i = 0; i < values.size(); i++) {
            records.add(new Record(i, 0L, null, values.get(i)));
        }
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        long now = System.currentTimeMillis();
        RecordBatch.encode(buf, 0L, 0, now, now, producerId, (short) epoch, baseSequence, records);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        var req = ProduceRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setBatch(ByteString.copyFrom(bytes))
                .setProducerId(producerId)
                .setProducerEpoch(epoch)
                .setBaseSequence(baseSequence)
                .setAcks(-1)
                .build();
        return produce(req, config.callDeadlineMs()).getLastOffset();
    }

    /**
     * Route a pre-built produce request to the partition leader, retrying
     * through NOT_LEADER and transport failures until {@code deadlineMs}.
     * The returned response is always error-free — every failure mode
     * either retries or throws.
     */
    public ProduceResponse produce(ProduceRequest req, long deadlineMs) {
        var tp = new Tp(req.getTopic(), req.getPartition());
        String what = "produce " + tp;
        long deadline = deadlineNanos(deadlineMs);
        RuntimeException last = null;
        boolean lastWasHintFollow = false;
        for (int attempt = 0; ; attempt++) {
            checkDeadline(deadline, what, last);
            var ep = leaderEndpoint(tp);
            if (ep == null) {
                last = new RuntimeException("no leader known for " + tp);
                lastWasHintFollow = false;
                backoff(attempt, deadline, what, last);
                continue;
            }
            ProduceResponse resp;
            try {
                resp = transportFor(ep).produce(req, config.rpcTimeoutMs());
            } catch (StatusRuntimeException e) {
                last = onTransportFailure(ep, e, what);
                lastWasHintFollow = false;
                refreshTopic(tp.topic(), ep);
                backoff(attempt, deadline, what, last);
                continue;
            }
            if (!isError(resp.hasError(), resp.getError().getCode())) return resp;
            int code = resp.getError().getCode();
            last = envelopeFailure(what, resp.getError());
            if (code == ErrorCodes.NOT_LEADER) {
                // Hints short-circuit the metadata refresh — but only one
                // hop without sleeping, so two brokers with mutually stale
                // views can't ping-pong the client in a hot loop.
                if (!lastWasHintFollow && applyLeaderHint(tp, resp.getError())) {
                    lastWasHintFollow = true;
                    continue;
                }
                lastWasHintFollow = false;
                leaders.remove(tp);
                refreshTopic(tp.topic(), null);
                backoff(attempt, deadline, what, last);
                continue;
            }
            lastWasHintFollow = false;
            if (retriableCode(code)) {
                backoff(attempt, deadline, what, last);
                continue;
            }
            throw last;
        }
    }

    // ---- Admin path (topic creation for cluster-aware apps) ----

    /**
     * Create a topic via the controller, following NOT_LEADER
     * {@code suggested_leader_*} hints. TOPIC_ALREADY_EXISTS on a retry
     * attempt counts as success — the previous attempt's propose may have
     * committed before its transport failed; on the first attempt it is a
     * genuine conflict and throws.
     */
    public void createTopic(String topic, int partitions, int replicationFactor) {
        var req = CreateTopicRequest.newBuilder()
                .setTopic(topic)
                .setPartitions(partitions)
                .setReplicationFactor(replicationFactor)
                .build();
        String what = "createTopic " + topic;
        long deadline = deadlineNanos(config.callDeadlineMs());
        RuntimeException last = null;
        boolean lastWasHintFollow = false;
        Endpoint hinted = null;
        for (int attempt = 0; ; attempt++) {
            checkDeadline(deadline, what, last);
            var ep = hinted != null ? hinted : controllerOrAnyEndpoint();
            hinted = null;
            if (ep == null) {
                last = new RuntimeException("no reachable broker for " + what);
                backoff(attempt, deadline, what, last);
                continue;
            }
            CreateTopicResponse resp;
            try {
                resp = transportFor(ep).createTopic(req, config.rpcTimeoutMs());
            } catch (StatusRuntimeException e) {
                last = onTransportFailure(ep, e, what);
                lastWasHintFollow = false;
                refreshClusterView(ep);
                backoff(attempt, deadline, what, last);
                continue;
            }
            if (!isError(resp.hasError(), resp.getError().getCode())) return;
            int code = resp.getError().getCode();
            if (code == ErrorCodes.TOPIC_ALREADY_EXISTS && attempt > 0) return;
            last = envelopeFailure(what, resp.getError());
            if (code == ErrorCodes.NOT_LEADER) {
                if (!lastWasHintFollow) {
                    hinted = hintedEndpoint(resp.getError());
                    if (hinted != null) {
                        lastWasHintFollow = true;
                        continue;
                    }
                }
                lastWasHintFollow = false;
                refreshClusterView(ep);
                backoff(attempt, deadline, what, last);
                continue;
            }
            lastWasHintFollow = false;
            if (retriableCode(code)) {
                backoff(attempt, deadline, what, last);
                continue;
            }
            throw last;
        }
    }

    // ---- Consumer path ----

    /**
     * Fetch routed to the partition leader (falling back through ISR
     * members while a leader is unknown), retrying transport failures until
     * {@code deadlineMs}. Envelope errors pass through untouched — fetch
     * sessions and offset state belong to the caller.
     */
    public FetchResponse fetch(FetchRequest req, long deadlineMs) {
        var tp = new Tp(req.getTopic(), req.getPartition());
        String what = "fetch " + tp;
        long deadline = deadlineNanos(deadlineMs);
        RuntimeException last = null;
        for (int attempt = 0; ; attempt++) {
            checkDeadline(deadline, what, last);
            var ep = leaderOrIsrEndpoint(tp);
            if (ep == null) {
                last = new RuntimeException("no reachable replica for " + tp);
                backoff(attempt, deadline, what, last);
                continue;
            }
            try {
                return transportFor(ep).fetch(req, config.rpcTimeoutMs());
            } catch (StatusRuntimeException e) {
                last = onTransportFailure(ep, e, what);
                refreshTopic(tp.topic(), ep);
                backoff(attempt, deadline, what, last);
            }
        }
    }

    /**
     * ListOffsets routed like {@link #fetch} using the request's first
     * partition. Per-result errors pass through.
     */
    public ListOffsetsResponse listOffsets(ListOffsetsRequest req, long deadlineMs) {
        if (req.getPartitionsCount() == 0) throw new IllegalArgumentException("listOffsets needs a partition");
        var first = req.getPartitions(0).getTp();
        var tp = new Tp(first.getTopic(), first.getPartition());
        String what = "listOffsets " + tp;
        long deadline = deadlineNanos(deadlineMs);
        RuntimeException last = null;
        for (int attempt = 0; ; attempt++) {
            checkDeadline(deadline, what, last);
            var ep = leaderOrIsrEndpoint(tp);
            if (ep == null) {
                last = new RuntimeException("no reachable replica for " + tp);
                backoff(attempt, deadline, what, last);
                continue;
            }
            try {
                return transportFor(ep).listOffsets(req, config.rpcTimeoutMs());
            } catch (StatusRuntimeException e) {
                last = onTransportFailure(ep, e, what);
                refreshTopic(tp.topic(), ep);
                backoff(attempt, deadline, what, last);
            }
        }
    }

    /**
     * Heartbeat routed to the group's coordinator. NOT_COORDINATOR /
     * COORDINATOR_NOT_AVAILABLE invalidate the cached coordinator and
     * re-find it; member-level errors (unknown member, fenced epoch, ...)
     * pass through — they are the consumer's state machine to handle.
     */
    public ConsumerGroupHeartbeatResponse consumerGroupHeartbeat(ConsumerGroupHeartbeatRequest req, long deadlineMs) {
        return coordinatorCall(
                req.getGroupId(),
                "heartbeat " + req.getGroupId(),
                deadlineMs,
                (t, timeout) -> t.consumerGroupHeartbeat(req, timeout),
                resp -> coordinatorMoved(resp.getError()));
    }

    /** CommitOffsets routed to the group's coordinator; see {@link #consumerGroupHeartbeat}. */
    public CommitOffsetsResponse commitOffsets(CommitOffsetsRequest req, long deadlineMs) {
        return coordinatorCall(
                req.getGroupId(),
                "commitOffsets " + req.getGroupId(),
                deadlineMs,
                (t, timeout) -> t.commitOffsets(req, timeout),
                resp -> resp.getResultsList().stream().anyMatch(r -> coordinatorMoved(r.getError())));
    }

    /** FetchOffsets routed to the group's coordinator; see {@link #consumerGroupHeartbeat}. */
    public FetchOffsetsResponse fetchOffsets(FetchOffsetsRequest req, long deadlineMs) {
        return coordinatorCall(
                req.getGroupId(),
                "fetchOffsets " + req.getGroupId(),
                deadlineMs,
                (t, timeout) -> t.fetchOffsets(req, timeout),
                resp -> resp.getResultsList().stream().anyMatch(r -> coordinatorMoved(r.getError())));
    }

    /** Drop the cached coordinator for {@code groupId}; the next call re-finds it. */
    public void invalidateCoordinator(String groupId) {
        coordinators.remove(groupId);
    }

    // ---- Lifecycle ----

    @Override
    public void close() {
        closed = true;
        for (var ep : List.copyOf(transports.keySet())) {
            closeTransport(ep);
        }
    }

    // ---- Routing internals ----

    private interface CoordinatorOp<T> {
        T call(Transport t, long timeoutMs);
    }

    private interface MovedCheck<T> {
        boolean moved(T resp);
    }

    private <T> T coordinatorCall(
            String groupId, String what, long deadlineMs, CoordinatorOp<T> op, MovedCheck<T> moved) {
        long deadline = deadlineNanos(deadlineMs);
        RuntimeException last = null;
        for (int attempt = 0; ; attempt++) {
            checkDeadline(deadline, what, last);
            var ep = coordinatorEndpoint(groupId);
            if (ep == null) {
                last = new RuntimeException("no coordinator found for group " + groupId);
                backoff(attempt, deadline, what, last);
                continue;
            }
            T resp;
            try {
                resp = op.call(transportFor(ep), config.rpcTimeoutMs());
            } catch (StatusRuntimeException e) {
                last = onTransportFailure(ep, e, what);
                coordinators.remove(groupId);
                backoff(attempt, deadline, what, last);
                continue;
            }
            if (moved.moved(resp)) {
                last = new RuntimeException(what + ": coordinator moved off " + ep);
                coordinators.remove(groupId);
                backoff(attempt, deadline, what, last);
                continue;
            }
            return resp;
        }
    }

    private static boolean coordinatorMoved(ErrorCode error) {
        return error == ErrorCode.NOT_COORDINATOR || error == ErrorCode.COORDINATOR_NOT_AVAILABLE;
    }

    /** Cached coordinator endpoint, else one FindCoordinator sweep over live brokers. */
    private Endpoint coordinatorEndpoint(String groupId) {
        var cached = coordinators.get(groupId);
        if (cached != null) return cached;
        var req = FindCoordinatorRequest.newBuilder().setKey(groupId).build();
        for (var ep : candidateEndpoints(null)) {
            FindCoordinatorResponse resp;
            try {
                resp = transportFor(ep).findCoordinator(req, METADATA_TIMEOUT_MS);
            } catch (StatusRuntimeException e) {
                if (!retriableTransport(e)) throw e;
                closeTransport(ep);
                continue;
            }
            if (resp.getError() != ErrorCode.OK) continue;
            var c = resp.getCoordinator();
            if (c.getHost().isEmpty() || c.getPort() <= 0) continue;
            var found = new Endpoint(c.getHost(), c.getPort());
            coordinators.put(groupId, found);
            return found;
        }
        return null;
    }

    /** Partition leader endpoint from the cache, refreshing once on a miss. */
    private Endpoint leaderEndpoint(Tp tp) {
        var ep = cachedLeaderEndpoint(tp);
        if (ep != null) return ep;
        refreshTopic(tp.topic(), null);
        return cachedLeaderEndpoint(tp);
    }

    private Endpoint cachedLeaderEndpoint(Tp tp) {
        var info = leaders.get(tp);
        if (info == null || info.leader() <= 0) return null;
        return view.brokers().get(info.leader());
    }

    /** Leader endpoint, else the first ISR member with a known endpoint. */
    private Endpoint leaderOrIsrEndpoint(Tp tp) {
        var ep = leaderEndpoint(tp);
        if (ep != null) return ep;
        var info = leaders.get(tp);
        if (info != null) {
            for (int isrMember : info.isr()) {
                var candidate = view.brokers().get(isrMember);
                if (candidate != null) return candidate;
            }
        }
        return null;
    }

    /** Controller endpoint when known, else the first reachable candidate. */
    private Endpoint controllerOrAnyEndpoint() {
        var v = view;
        var controller = v.brokers().get(v.controllerId());
        if (controller != null) return controller;
        refreshClusterView(null);
        v = view;
        controller = v.brokers().get(v.controllerId());
        if (controller != null) return controller;
        var candidates = candidateEndpoints(null);
        return candidates.isEmpty() ? null : candidates.get(0);
    }

    /**
     * Endpoints to try for metadata and rotation, in preference order:
     * brokers the view says are alive, then the rest of the view, then the
     * bootstrap list — always excluding the endpoint that just failed.
     */
    private List<Endpoint> candidateEndpoints(Endpoint excluding) {
        var v = view;
        var out = new LinkedHashSet<Endpoint>();
        for (var e : v.brokers().entrySet()) {
            if (Boolean.TRUE.equals(v.alive().get(e.getKey()))) out.add(e.getValue());
        }
        out.addAll(v.brokers().values());
        out.addAll(bootstrap);
        if (excluding != null) out.remove(excluding);
        return List.copyOf(out);
    }

    /**
     * Refresh the broker-level view via the first candidate that answers
     * DescribeCluster (skipping {@code failed}). Best-effort: leaves the
     * old view in place when nobody answers. Channels of brokers that
     * dropped out of the view (and are not bootstrap endpoints) are closed.
     */
    private void refreshClusterView(Endpoint failed) {
        synchronized (refreshLock) {
            for (var ep : candidateEndpoints(failed)) {
                DescribeClusterResponse resp;
                try {
                    resp = transportFor(ep).describeCluster(METADATA_TIMEOUT_MS);
                } catch (StatusRuntimeException e) {
                    if (!retriableTransport(e)) throw e;
                    closeTransport(ep);
                    continue;
                }
                if (resp.getError() != ErrorCode.OK) continue;
                applyClusterView(resp);
                return;
            }
        }
    }

    private void applyClusterView(DescribeClusterResponse resp) {
        var old = view;
        var brokers = new java.util.HashMap<Integer, Endpoint>();
        var alive = new java.util.HashMap<Integer, Boolean>();
        for (var node : resp.getNodesList()) {
            Endpoint ep;
            if (node.getHost().isEmpty() || node.getPort() <= 0) {
                // Registry had no advertised address (yet); keep the last
                // known endpoint rather than forgetting the broker.
                ep = old.brokers().get(node.getBrokerId());
                if (ep == null) continue;
            } else {
                ep = new Endpoint(node.getHost(), node.getPort());
            }
            brokers.put(node.getBrokerId(), ep);
            alive.put(node.getBrokerId(), node.getAlive());
        }
        view = new ClusterView(Map.copyOf(brokers), Map.copyOf(alive), resp.getControllerId());
        // A broker that disappeared from the view no longer earns a live
        // channel; bootstrap endpoints stay dialable for fallback.
        for (var cached : List.copyOf(transports.keySet())) {
            if (!brokers.containsValue(cached) && !bootstrap.contains(cached)) {
                closeTransport(cached);
            }
        }
    }

    /**
     * Refresh one topic's partition leaders via the first candidate that
     * answers (skipping {@code failed}). Refreshes the broker view first
     * when the reported leader has no known endpoint. Best-effort.
     */
    private void refreshTopic(String topic, Endpoint failed) {
        synchronized (refreshLock) {
            for (var ep : candidateEndpoints(failed)) {
                DescribeTopicPartitionsResponse resp;
                try {
                    resp = transportFor(ep).describeTopicPartitions(topic, METADATA_TIMEOUT_MS);
                } catch (StatusRuntimeException e) {
                    if (!retriableTransport(e)) throw e;
                    closeTransport(ep);
                    continue;
                }
                if (resp.getError() != ErrorCode.OK) continue;
                boolean missingEndpoint = false;
                for (var ps : resp.getPartitionStatesList()) {
                    var tp = new Tp(topic, ps.getPartition());
                    if (ps.getLeader() <= 0) {
                        leaders.remove(tp); // election in flight — don't route on a stale leader
                    } else {
                        leaders.put(tp, new PartitionInfo(ps.getLeader(), List.copyOf(ps.getIsrList())));
                        if (!view.brokers().containsKey(ps.getLeader())) missingEndpoint = true;
                    }
                }
                if (missingEndpoint) refreshClusterView(failed);
                return;
            }
        }
    }

    /**
     * Apply a NOT_LEADER error's {@code suggested_leader_*} hints to the
     * cache. Returns true when the hint pinned a dialable leader — id plus
     * either a hinted address or an endpoint already in the view.
     */
    private boolean applyLeaderHint(Tp tp, jbroker.proto.broker.Error error) {
        var hints = error.getHintMap();
        int leaderId = parseIntHint(hints.get("suggested_leader_id"));
        if (leaderId <= 0) return false;
        var hinted = hintedEndpoint(error);
        if (hinted != null) {
            var old = view;
            var brokers = new java.util.HashMap<>(old.brokers());
            brokers.put(leaderId, hinted);
            var alive = new java.util.HashMap<>(old.alive());
            alive.put(leaderId, true);
            synchronized (refreshLock) {
                view = new ClusterView(Map.copyOf(brokers), Map.copyOf(alive), old.controllerId());
            }
        } else if (!view.brokers().containsKey(leaderId)) {
            return false;
        }
        var prior = leaders.get(tp);
        leaders.put(tp, new PartitionInfo(leaderId, prior == null ? List.of() : prior.isr()));
        return true;
    }

    /** The hinted leader's endpoint, or null when host/port hints are absent or malformed. */
    private static Endpoint hintedEndpoint(jbroker.proto.broker.Error error) {
        var hints = error.getHintMap();
        String host = hints.get("suggested_leader_host");
        int port = parseIntHint(hints.get("suggested_leader_port"));
        if (host == null || host.isEmpty() || port <= 0) return null;
        return new Endpoint(host, port);
    }

    private static int parseIntHint(String s) {
        if (s == null) return -1;
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    // ---- Transport cache ----

    private Transport transportFor(Endpoint ep) {
        if (closed) throw new IllegalStateException("client is closed");
        return transports.computeIfAbsent(ep, factory::connect);
    }

    private void closeTransport(Endpoint ep) {
        var t = transports.remove(ep);
        if (t != null) t.close();
    }

    /**
     * Classify a transport failure: UNAVAILABLE / DEADLINE_EXCEEDED drop
     * the (possibly dead) channel and are retried after rotation; anything
     * else propagates.
     */
    private RuntimeException onTransportFailure(Endpoint ep, StatusRuntimeException e, String what) {
        if (!retriableTransport(e)) throw e;
        closeTransport(ep);
        return new RuntimeException(what + " failed against " + ep + ": " + e.getStatus(), e);
    }

    private static boolean retriableTransport(StatusRuntimeException e) {
        var code = e.getStatus().getCode();
        return code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED;
    }

    /**
     * Broker-envelope retriability, following the {@link ErrorCodes}
     * taxonomy: leadership and capacity conditions heal on their own;
     * everything else can never succeed by retrying the same request.
     */
    private static boolean retriableCode(int code) {
        return code == ErrorCodes.NOT_LEADER
                || code == ErrorCodes.NOT_ENOUGH_REPLICAS
                || code == ErrorCodes.STORAGE_FULL
                || code == ErrorCodes.QUOTA_VIOLATED
                || code == ErrorCodes.COORDINATOR_NOT_AVAILABLE
                || code == ErrorCodes.NOT_COORDINATOR;
    }

    private static boolean isError(boolean hasError, int code) {
        return hasError && code != 0;
    }

    private static RuntimeException envelopeFailure(String what, jbroker.proto.broker.Error error) {
        return new RuntimeException(
                what + " failed: " + ErrorCodeNames.name(error.getCode()) + " (" + error.getMessage() + ")");
    }

    // ---- Backoff engine ----

    private long deadlineNanos(long deadlineMs) {
        return ticker.nanoTime() + TimeUnit.MILLISECONDS.toNanos(deadlineMs);
    }

    private void checkDeadline(long deadlineNanos, String what, RuntimeException last) {
        if (ticker.nanoTime() >= deadlineNanos) {
            throw new RetriesExhaustedException(what + ": retries exhausted", last);
        }
    }

    /**
     * Sleep the attempt's backoff: equal jitter over
     * {@code raw = min(cap, base * mult^attempt)}, i.e. uniform in
     * {@code [raw/2, raw]}. Throws {@link RetriesExhaustedException} when
     * the sleep would cross the deadline — no point waking up past it.
     */
    private void backoff(int attempt, long deadlineNanos, String what, RuntimeException last) {
        double raw = config.backoffBaseMs() * Math.pow(config.backoffMultiplier(), attempt);
        long rawMs = (long) Math.min(config.backoffCapMs(), raw);
        rawMs = Math.max(1, rawMs);
        long half = rawMs / 2;
        long sleepMs = half + random.nextLong(rawMs - half + 1);
        if (ticker.nanoTime() + TimeUnit.MILLISECONDS.toNanos(sleepMs) >= deadlineNanos) {
            throw new RetriesExhaustedException(what + ": retries exhausted", last);
        }
        try {
            ticker.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetriesExhaustedException(what + ": interrupted during backoff", last);
        }
    }

    // ---- gRPC transport ----

    private static TransportFactory grpcFactory(TlsConfig tls) {
        return endpoint -> new GrpcTransport(endpoint, tls);
    }

    /** Real per-broker transport: one channel, blocking stubs per service. */
    private static final class GrpcTransport implements Transport {

        private final ManagedChannel channel;
        private final ProducerGrpc.ProducerBlockingStub producer;
        private final ConsumerGrpc.ConsumerBlockingStub consumer;
        private final AdminGrpc.AdminBlockingStub admin;
        private final MetadataGrpc.MetadataBlockingStub metadata;

        GrpcTransport(Endpoint endpoint, TlsConfig tls) {
            var b = NettyChannelBuilder.forAddress(endpoint.host(), endpoint.port());
            try {
                var sslCtx = TlsContexts.clientContext(tls);
                if (sslCtx == null) {
                    b.usePlaintext();
                } else {
                    b.sslContext(sslCtx);
                }
            } catch (javax.net.ssl.SSLException e) {
                throw new IllegalStateException("TLS client context build failed", e);
            }
            this.channel = b.build();
            this.producer = ProducerGrpc.newBlockingStub(channel);
            this.consumer = ConsumerGrpc.newBlockingStub(channel);
            this.admin = AdminGrpc.newBlockingStub(channel);
            this.metadata = MetadataGrpc.newBlockingStub(channel);
        }

        @Override
        public DescribeClusterResponse describeCluster(long timeoutMs) {
            return metadata.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                    .describeCluster(DescribeClusterRequest.getDefaultInstance());
        }

        @Override
        public DescribeTopicPartitionsResponse describeTopicPartitions(String topic, long timeoutMs) {
            return metadata.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                    .describeTopicPartitions(DescribeTopicPartitionsRequest.newBuilder()
                            .setTopic(topic)
                            .build());
        }

        @Override
        public InitProducerIdResponse initProducerId(long timeoutMs) {
            return producer.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                    .initProducerId(InitProducerIdRequest.getDefaultInstance());
        }

        @Override
        public ProduceResponse produce(ProduceRequest req, long timeoutMs) {
            return producer.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).produce(req);
        }

        @Override
        public FetchResponse fetch(FetchRequest req, long timeoutMs) {
            return consumer.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).fetch(req);
        }

        @Override
        public ListOffsetsResponse listOffsets(ListOffsetsRequest req, long timeoutMs) {
            return consumer.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).listOffsets(req);
        }

        @Override
        public FindCoordinatorResponse findCoordinator(FindCoordinatorRequest req, long timeoutMs) {
            return consumer.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).findCoordinator(req);
        }

        @Override
        public ConsumerGroupHeartbeatResponse consumerGroupHeartbeat(
                ConsumerGroupHeartbeatRequest req, long timeoutMs) {
            return consumer.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).consumerGroupHeartbeat(req);
        }

        @Override
        public CommitOffsetsResponse commitOffsets(CommitOffsetsRequest req, long timeoutMs) {
            return consumer.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).commitOffsets(req);
        }

        @Override
        public FetchOffsetsResponse fetchOffsets(FetchOffsetsRequest req, long timeoutMs) {
            return consumer.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).fetchOffsets(req);
        }

        @Override
        public CreateTopicResponse createTopic(CreateTopicRequest req, long timeoutMs) {
            return admin.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).createTopic(req);
        }

        @Override
        public void close() {
            channel.shutdown();
            try {
                channel.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
