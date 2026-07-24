package jbroker.broker.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import jbroker.broker.client.ClusterClient.Endpoint;
import jbroker.proto.broker.BrokerInfo;
import jbroker.proto.broker.CommitOffsetsRequest;
import jbroker.proto.broker.CommitOffsetsResponse;
import jbroker.proto.broker.CommitResult;
import jbroker.proto.broker.ConsumerGroupHeartbeatRequest;
import jbroker.proto.broker.ConsumerGroupHeartbeatResponse;
import jbroker.proto.broker.CreateTopicRequest;
import jbroker.proto.broker.CreateTopicResponse;
import jbroker.proto.broker.DescribeClusterResponse;
import jbroker.proto.broker.DescribeTopicPartitionsResponse;
import jbroker.proto.broker.Error;
import jbroker.proto.broker.FetchOffsetsRequest;
import jbroker.proto.broker.FetchOffsetsResponse;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.broker.FetchResponse;
import jbroker.proto.broker.FindCoordinatorRequest;
import jbroker.proto.broker.FindCoordinatorResponse;
import jbroker.proto.broker.InitProducerIdResponse;
import jbroker.proto.broker.ListOffsetsRequest;
import jbroker.proto.broker.ListOffsetsResponse;
import jbroker.proto.broker.PartitionStateInfo;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ProduceResponse;
import jbroker.proto.common.BrokerEndpoint;
import jbroker.proto.common.ErrorCode;
import org.junit.jupiter.api.Test;

/**
 * Routing, cache-refresh, and backoff logic of {@link ClusterClient},
 * exercised against a scripted in-memory cluster — no broker, no real
 * clock. The fake mirrors the real handlers' error shapes: produce-path
 * NOT_LEADER carries no hints (like ProduceHandler), admin-path NOT_LEADER
 * carries {@code suggested_leader_*} hints (like AdminHandler).
 */
class ClusterClientTest {

    private static final Endpoint B1 = new Endpoint("h1", 1001);
    private static final Endpoint B2 = new Endpoint("h2", 1002);
    private static final Endpoint B3 = new Endpoint("h3", 1003);

    // ---- fake cluster ----

    /** Shared mutable cluster model; per-endpoint transports read it live. */
    private static final class World {
        final Map<Integer, Endpoint> brokers = new LinkedHashMap<>();
        final Set<Integer> down = new HashSet<>();
        /** Brokers still dialable but no longer reported by DescribeCluster (decommissioned). */
        final Set<Integer> hiddenFromView = new HashSet<>();

        final Map<String, Integer> partitionLeaders = new HashMap<>(); // topic -> leader of partition 0
        int controllerId = 1;
        int coordinatorId = -1;
        long nextOffset;
        /** Scripted overrides, key {@code "<brokerId>:<op>"}; entries are responses or exceptions to throw. */
        final Map<String, ArrayDeque<Object>> scripts = new HashMap<>();
        /** Every transport call, {@code "<brokerId>:<op>"}, in order. */
        final List<String> calls = new ArrayList<>();

        void script(int brokerId, String op, Object outcome) {
            scripts.computeIfAbsent(brokerId + ":" + op, k -> new ArrayDeque<>())
                    .add(outcome);
        }

        int idOf(Endpoint ep) {
            for (var e : brokers.entrySet()) {
                if (e.getValue().equals(ep)) return e.getKey();
            }
            return -1;
        }

        List<String> callsSince(int mark) {
            return List.copyOf(calls.subList(mark, calls.size()));
        }
    }

    private static final class FakeTransport implements ClusterClient.Transport {
        final World world;
        final Endpoint endpoint;
        boolean closed;

        FakeTransport(World world, Endpoint endpoint) {
            this.world = world;
            this.endpoint = endpoint;
        }

        @SuppressWarnings("unchecked")
        private <T> T dispatch(String op, java.util.function.Supplier<T> fallback) {
            int id = world.idOf(endpoint);
            world.calls.add(id + ":" + op);
            if (id < 0 || world.down.contains(id)) {
                throw io.grpc.Status.UNAVAILABLE
                        .withDescription("down: " + endpoint)
                        .asRuntimeException();
            }
            var queue = world.scripts.get(id + ":" + op);
            if (queue != null && !queue.isEmpty()) {
                var outcome = queue.poll();
                if (outcome instanceof RuntimeException e) throw e;
                return (T) outcome;
            }
            return fallback.get();
        }

        @Override
        public DescribeClusterResponse describeCluster(long timeoutMs) {
            return dispatch("describeCluster", () -> {
                var b = DescribeClusterResponse.newBuilder()
                        .setError(ErrorCode.OK)
                        .setControllerId(world.controllerId);
                for (var e : world.brokers.entrySet()) {
                    if (world.hiddenFromView.contains(e.getKey())) continue;
                    b.addNodes(BrokerInfo.newBuilder()
                            .setBrokerId(e.getKey())
                            .setHost(e.getValue().host())
                            .setPort(e.getValue().port())
                            .setAlive(!world.down.contains(e.getKey()))
                            .build());
                }
                return b.build();
            });
        }

        @Override
        public DescribeTopicPartitionsResponse describeTopicPartitions(String topic, long timeoutMs) {
            return dispatch("describeTopicPartitions", () -> DescribeTopicPartitionsResponse.newBuilder()
                    .setError(ErrorCode.OK)
                    .setTopic(topic)
                    .setPartitions(1)
                    .addPartitionStates(PartitionStateInfo.newBuilder()
                            .setPartition(0)
                            .setLeader(world.partitionLeaders.getOrDefault(topic, -1))
                            .addAllIsr(world.brokers.keySet())
                            .build())
                    .build());
        }

        @Override
        public InitProducerIdResponse initProducerId(long timeoutMs) {
            return dispatch("initProducerId", () -> {
                if (world.controllerId != world.idOf(endpoint)) {
                    // Like the real handler: NOT_LEADER without hints.
                    return InitProducerIdResponse.newBuilder()
                            .setError(notLeaderNoHints())
                            .build();
                }
                return InitProducerIdResponse.newBuilder().setProducerId(7L).build();
            });
        }

        @Override
        public ProduceResponse produce(ProduceRequest req, long timeoutMs) {
            return dispatch("produce", () -> {
                if (world.partitionLeaders.getOrDefault(req.getTopic(), -1) != world.idOf(endpoint)) {
                    // Like ProduceHandler: NOT_LEADER, no hints.
                    return ProduceResponse.newBuilder()
                            .setError(notLeaderNoHints())
                            .build();
                }
                long last = world.nextOffset;
                world.nextOffset++;
                return ProduceResponse.newBuilder().setLastOffset(last).build();
            });
        }

        @Override
        public FetchResponse fetch(FetchRequest req, long timeoutMs) {
            return dispatch("fetch", () -> FetchResponse.newBuilder().build());
        }

        @Override
        public ListOffsetsResponse listOffsets(ListOffsetsRequest req, long timeoutMs) {
            return dispatch(
                    "listOffsets", () -> ListOffsetsResponse.newBuilder().build());
        }

        @Override
        public FindCoordinatorResponse findCoordinator(FindCoordinatorRequest req, long timeoutMs) {
            return dispatch("findCoordinator", () -> {
                var coord = world.brokers.get(world.coordinatorId);
                if (coord == null || world.down.contains(world.coordinatorId)) {
                    return FindCoordinatorResponse.newBuilder()
                            .setError(ErrorCode.COORDINATOR_NOT_AVAILABLE)
                            .build();
                }
                return FindCoordinatorResponse.newBuilder()
                        .setError(ErrorCode.OK)
                        .setCoordinator(BrokerEndpoint.newBuilder()
                                .setNodeId(world.coordinatorId)
                                .setHost(coord.host())
                                .setPort(coord.port())
                                .build())
                        .build();
            });
        }

        @Override
        public ConsumerGroupHeartbeatResponse consumerGroupHeartbeat(
                ConsumerGroupHeartbeatRequest req, long timeoutMs) {
            return dispatch("heartbeat", () -> {
                if (world.coordinatorId != world.idOf(endpoint)) {
                    return ConsumerGroupHeartbeatResponse.newBuilder()
                            .setError(ErrorCode.NOT_COORDINATOR)
                            .build();
                }
                return ConsumerGroupHeartbeatResponse.newBuilder()
                        .setError(ErrorCode.OK)
                        .setMemberId("m-1")
                        .setMemberEpoch(1)
                        .build();
            });
        }

        @Override
        public CommitOffsetsResponse commitOffsets(CommitOffsetsRequest req, long timeoutMs) {
            return dispatch("commitOffsets", () -> {
                var b = CommitOffsetsResponse.newBuilder();
                var error = world.coordinatorId == world.idOf(endpoint) ? ErrorCode.OK : ErrorCode.NOT_COORDINATOR;
                for (var c : req.getCommitsList()) {
                    b.addResults(CommitResult.newBuilder().setTp(c.getTp()).setError(error));
                }
                return b.build();
            });
        }

        @Override
        public FetchOffsetsResponse fetchOffsets(FetchOffsetsRequest req, long timeoutMs) {
            return dispatch(
                    "fetchOffsets", () -> FetchOffsetsResponse.newBuilder().build());
        }

        @Override
        public CreateTopicResponse createTopic(CreateTopicRequest req, long timeoutMs) {
            return dispatch("createTopic", () -> {
                int selfId = world.idOf(endpoint);
                if (world.controllerId != selfId) {
                    // Like AdminHandler: NOT_LEADER WITH suggested_leader hints.
                    var controller = world.brokers.get(world.controllerId);
                    var err = Error.newBuilder()
                            .setCode(jbroker.broker.ErrorCodes.NOT_LEADER)
                            .setMessage("not the controller; leader is broker " + world.controllerId)
                            .putHint("suggested_leader_id", Integer.toString(world.controllerId));
                    if (controller != null) {
                        err.putHint("suggested_leader_host", controller.host());
                        err.putHint("suggested_leader_port", Integer.toString(controller.port()));
                    }
                    return CreateTopicResponse.newBuilder().setError(err).build();
                }
                return CreateTopicResponse.newBuilder().build();
            });
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static Error notLeaderNoHints() {
        return Error.newBuilder()
                .setCode(jbroker.broker.ErrorCodes.NOT_LEADER)
                .setMessage("not leader")
                .build();
    }

    private static Error retriable(int code, String message) {
        return Error.newBuilder().setCode(code).setMessage(message).build();
    }

    /** Manual clock: only {@link #sleep} advances it; sleeps are recorded. */
    private static final class FakeTicker implements ClusterClient.Ticker {
        final AtomicLong nanos = new AtomicLong();
        final List<Long> sleepsMs = new ArrayList<>();
        Runnable onSleep = () -> {};

        @Override
        public long nanoTime() {
            return nanos.get();
        }

        @Override
        public void sleep(long millis) {
            sleepsMs.add(millis);
            nanos.addAndGet(millis * 1_000_000L);
            onSleep.run();
        }
    }

    private static final class Harness {
        final World world = new World();
        final FakeTicker ticker = new FakeTicker();
        final Map<Endpoint, FakeTransport> transports = new HashMap<>();
        ClusterClient client;

        Harness threeBrokers() {
            world.brokers.put(1, B1);
            world.brokers.put(2, B2);
            world.brokers.put(3, B3);
            return this;
        }

        ClusterClient client(List<Endpoint> bootstrap, ClusterClient.Config config) {
            client = new ClusterClient(
                    bootstrap,
                    config,
                    ep -> {
                        var t = new FakeTransport(world, ep);
                        transports.put(ep, t);
                        return t;
                    },
                    ticker,
                    new Random(42));
            return client;
        }

        ClusterClient client() {
            return client(List.of(B1), config(30_000));
        }
    }

    private static ClusterClient.Config config(long callDeadlineMs) {
        return new ClusterClient.Config(100, 2.0, 1_000, callDeadlineMs, 1_000);
    }

    private static long produceOne(ClusterClient client, String topic) {
        return client.produceIdempotentBatchAcksAll(topic, 0, List.of("v".getBytes(StandardCharsets.UTF_8)), 7L, 0, 0);
    }

    // ---- bootstrap + routing ----

    @Test
    void bootstrapWalksEndpointListUntilOneAnswers() {
        var h = new Harness().threeBrokers();
        h.world.partitionLeaders.put("t", 2);
        // First bootstrap endpoint is dead (not a cluster member at all);
        // the client must move on to the next and still find the leader.
        var dead = new Endpoint("dead", 9999);
        var client = h.client(List.of(dead, B1), config(30_000));

        assertThat(produceOne(client, "t")).isEqualTo(0L);

        assertThat(h.world.calls).contains("2:produce");
        // The dead endpoint was dialed before B1 (order preserved) and
        // never received a produce.
        assertThat(h.world.calls.get(0)).startsWith("-1:");
        assertThat(h.world.calls).doesNotContain("-1:produce");
    }

    @Test
    void produceRoutesFromCacheWithoutReDescribing() {
        var h = new Harness().threeBrokers();
        h.world.partitionLeaders.put("t", 3);
        var client = h.client();

        produceOne(client, "t");
        int mark = h.world.calls.size();
        produceOne(client, "t");

        assertThat(h.world.callsSince(mark))
                .as("second produce goes straight to the cached leader")
                .containsExactly("3:produce");
    }

    // ---- NOT_LEADER handling ----

    @Test
    void notLeaderHintShortCircuitsToHintedBrokerWithoutRefreshOrSleep() {
        var h = new Harness().threeBrokers();
        h.world.partitionLeaders.put("t", 1);
        var client = h.client();
        produceOne(client, "t"); // warm the caches, leader = broker 1

        // Broker 1 rejects once, hinting broker 2 (admin-style hint shape);
        // the world agrees broker 2 is now the leader.
        h.world.script(
                1,
                "produce",
                ProduceResponse.newBuilder()
                        .setError(Error.newBuilder()
                                .setCode(jbroker.broker.ErrorCodes.NOT_LEADER)
                                .setMessage("leader moved")
                                .putHint("suggested_leader_id", "2")
                                .putHint("suggested_leader_host", B2.host())
                                .putHint("suggested_leader_port", Integer.toString(B2.port())))
                        .build());
        h.world.partitionLeaders.put("t", 2);

        int mark = h.world.calls.size();
        produceOne(client, "t");

        assertThat(h.world.callsSince(mark))
                .as("hint short-circuits: no metadata RPC between the two attempts")
                .containsExactly("1:produce", "2:produce");
        assertThat(h.ticker.sleepsMs).as("hint follow does not back off").isEmpty();
    }

    @Test
    void notLeaderWithoutHintRefreshesTopicMetadataThenRetries() {
        var h = new Harness().threeBrokers();
        h.world.partitionLeaders.put("t", 1);
        var client = h.client();
        produceOne(client, "t");

        h.world.script(
                1,
                "produce",
                ProduceResponse.newBuilder().setError(notLeaderNoHints()).build());
        h.world.partitionLeaders.put("t", 3);

        int mark = h.world.calls.size();
        produceOne(client, "t");

        var since = h.world.callsSince(mark);
        assertThat(since.get(0)).isEqualTo("1:produce");
        assertThat(since)
                .as("no hint: the client re-describes the topic before retrying")
                .anyMatch(c -> c.endsWith(":describeTopicPartitions"));
        assertThat(since.get(since.size() - 1)).isEqualTo("3:produce");
        assertThat(h.ticker.sleepsMs).hasSize(1);
    }

    @Test
    void electionWindowRetriesUntilLeaderAppears() {
        var h = new Harness().threeBrokers();
        // No leader yet: DescribeTopicPartitions reports leader -1.
        var client = h.client();
        // After two backoffs the election completes.
        h.ticker.onSleep = () -> {
            if (h.ticker.sleepsMs.size() == 2) h.world.partitionLeaders.put("t", 2);
        };

        assertThat(produceOne(client, "t")).isEqualTo(0L);
        assertThat(h.ticker.sleepsMs).hasSize(2);
        assertThat(h.world.calls).contains("2:produce");
    }

    // ---- transport failures ----

    @Test
    void unavailableLeaderRotatesRefreshesAndRetries() {
        var h = new Harness().threeBrokers();
        h.world.partitionLeaders.put("t", 1);
        var client = h.client();
        produceOne(client, "t");

        // Kill broker 1; the cluster elects broker 2.
        h.world.down.add(1);
        h.world.partitionLeaders.put("t", 2);

        int mark = h.world.calls.size();
        assertThat(produceOne(client, "t")).isEqualTo(1L);

        var since = h.world.callsSince(mark);
        assertThat(since.get(0)).isEqualTo("1:produce"); // the UNAVAILABLE attempt
        assertThat(since.subList(1, since.size()))
                .as("refresh + retry rotate away from the dead broker")
                .noneMatch(c -> c.startsWith("1:"));
        assertThat(since.get(since.size() - 1)).isEqualTo("2:produce");
        // The dead broker's channel was dropped.
        assertThat(h.transports.get(B1).closed).isTrue();
    }

    @Test
    void serverSentStreamResetRotatesLikeUnavailable() {
        var h = new Harness().threeBrokers();
        h.world.partitionLeaders.put("t", 1);
        var client = h.client();
        produceOne(client, "t");

        // An abrupt broker death mid-RPC surfaces as CANCELLED
        // (RST_STREAM), not UNAVAILABLE — it must rotate all the same.
        h.world.script(
                1,
                "produce",
                io.grpc.Status.CANCELLED
                        .withDescription("RST_STREAM closed stream. HTTP/2 error code: CANCEL")
                        .asRuntimeException());
        h.world.partitionLeaders.put("t", 3);

        assertThat(produceOne(client, "t")).isEqualTo(1L);
        assertThat(h.world.calls).contains("3:produce");
        assertThat(h.transports.get(B1).closed).isTrue();
    }

    // ---- retriable vs fatal envelope codes ----

    @Test
    void notEnoughReplicasBacksOffWithGrowingJitteredSleeps() {
        var h = new Harness().threeBrokers();
        h.world.partitionLeaders.put("t", 1);
        var client = h.client();
        for (int i = 0; i < 6; i++) {
            h.world.script(
                    1,
                    "produce",
                    ProduceResponse.newBuilder()
                            .setError(retriable(jbroker.broker.ErrorCodes.NOT_ENOUGH_REPLICAS, "isr below floor"))
                            .build());
        }

        assertThat(produceOne(client, "t")).isEqualTo(0L);

        // Equal jitter over raw = min(1000, 100 * 2^k): each sleep lands in
        // [raw/2, raw], and raw doubles until the cap.
        long[] raws = {100, 200, 400, 800, 1_000, 1_000};
        assertThat(h.ticker.sleepsMs).hasSize(raws.length);
        for (int k = 0; k < raws.length; k++) {
            assertThat(h.ticker.sleepsMs.get(k))
                    .as("sleep %d within jitter bounds of raw=%d", k, raws[k])
                    .isBetween(raws[k] / 2, raws[k]);
        }
    }

    @Test
    void deadlineExhaustionCompletesExceptionallyWithLastFailure() {
        var h = new Harness().threeBrokers();
        h.world.partitionLeaders.put("t", 1);
        var client = h.client(List.of(B1), config(3_000));
        // Never recovers: every attempt is NOT_ENOUGH_REPLICAS.
        h.ticker.onSleep = () -> h.world.script(
                1,
                "produce",
                ProduceResponse.newBuilder()
                        .setError(retriable(jbroker.broker.ErrorCodes.NOT_ENOUGH_REPLICAS, "isr below floor"))
                        .build());
        h.world.script(
                1,
                "produce",
                ProduceResponse.newBuilder()
                        .setError(retriable(jbroker.broker.ErrorCodes.NOT_ENOUGH_REPLICAS, "isr below floor"))
                        .build());

        assertThatThrownBy(() -> produceOne(client, "t"))
                .isInstanceOf(ClusterClient.RetriesExhaustedException.class)
                .hasMessageContaining("produce t-0")
                .cause()
                .hasMessageContaining("NOT_ENOUGH_REPLICAS");
        // No sleep may cross the deadline.
        long sleptMs = h.ticker.sleepsMs.stream().mapToLong(Long::longValue).sum();
        assertThat(sleptMs).isLessThan(3_000);
    }

    @Test
    void fatalCodeFailsFastWithoutRetryOrSleep() {
        var h = new Harness().threeBrokers();
        h.world.partitionLeaders.put("t", 1);
        var client = h.client();
        produceOne(client, "t");
        h.world.script(
                1,
                "produce",
                ProduceResponse.newBuilder()
                        .setError(retriable(jbroker.broker.ErrorCodes.MESSAGE_TOO_LARGE, "1 MiB over cap"))
                        .build());

        int mark = h.world.calls.size();
        assertThatThrownBy(() -> produceOne(client, "t"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("MESSAGE_TOO_LARGE");
        assertThat(h.world.callsSince(mark)).containsExactly("1:produce");
        assertThat(h.ticker.sleepsMs).isEmpty();
    }

    // ---- controller-routed calls ----

    @Test
    void initProducerIdFollowsControllerAcrossNotLeader() {
        var h = new Harness().threeBrokers();
        var client = h.client();
        // Cached view says controller 1, but the controller has moved to 2
        // (the InitProducerId path has no hints, so this takes a refresh).
        assertThat(client.initProducerId()).isEqualTo(7L);
        h.world.script(
                1,
                "initProducerId",
                InitProducerIdResponse.newBuilder().setError(notLeaderNoHints()).build());
        h.world.controllerId = 2;

        assertThat(client.initProducerId()).isEqualTo(7L);
        assertThat(h.world.calls).contains("2:initProducerId");
    }

    @Test
    void createTopicFollowsSuggestedLeaderHintWithoutSleep() {
        var h = new Harness().threeBrokers();
        // Warm the view while broker 3 is controller, then move the
        // controller: the stale cache routes to broker 3, whose NOT_LEADER
        // answer carries admin-style hints pointing at broker 1.
        h.world.controllerId = 3;
        var client = h.client(List.of(B3), config(30_000));
        assertThat(client.initProducerId()).isEqualTo(7L);
        h.world.controllerId = 1;

        int mark = h.world.calls.size();
        client.createTopic("t", 1, 3);

        assertThat(h.world.callsSince(mark))
                .as("hint short-circuits: no metadata RPC between the two attempts")
                .containsExactly("3:createTopic", "1:createTopic");
        assertThat(h.ticker.sleepsMs).as("hint follow does not back off").isEmpty();
    }

    @Test
    void createTopicAlreadyExistsCountsAsSuccessOnlyOnRetry() {
        var h = new Harness().threeBrokers();
        var client = h.client();
        // First attempt dies in transport after the propose committed; the
        // retry's TOPIC_ALREADY_EXISTS means "already done", not conflict.
        h.world.script(
                1,
                "createTopic",
                io.grpc.Status.UNAVAILABLE.withDescription("cut").asRuntimeException());
        h.world.script(
                1,
                "createTopic",
                CreateTopicResponse.newBuilder()
                        .setError(retriable(jbroker.broker.ErrorCodes.TOPIC_ALREADY_EXISTS, "exists"))
                        .build());
        client.createTopic("t", 1, 3);

        // A first-attempt conflict is genuine and throws.
        h.world.script(
                1,
                "createTopic",
                CreateTopicResponse.newBuilder()
                        .setError(retriable(jbroker.broker.ErrorCodes.TOPIC_ALREADY_EXISTS, "exists"))
                        .build());
        assertThatThrownBy(() -> client.createTopic("other", 1, 3)).hasMessageContaining("TOPIC_ALREADY_EXISTS");
    }

    // ---- view lifecycle ----

    @Test
    void brokerDroppedFromClusterViewGetsItsChannelClosed() {
        var h = new Harness().threeBrokers();
        h.world.partitionLeaders.put("t", 3);
        var client = h.client();
        produceOne(client, "t"); // opens broker 3's channel

        // Broker 3 is decommissioned (still dialable, but DescribeCluster
        // no longer reports it); the controller moves and the next refresh
        // drops it from the view.
        h.world.hiddenFromView.add(3);
        h.world.partitionLeaders.put("t", 1);
        h.world.script(
                1,
                "initProducerId",
                InitProducerIdResponse.newBuilder().setError(notLeaderNoHints()).build());
        h.world.controllerId = 2;

        assertThat(client.initProducerId()).isEqualTo(7L);
        assertThat(h.transports.get(B3).closed)
                .as("channel of a broker gone from the view is closed")
                .isTrue();
    }

    @Test
    void closeShutsDownEveryCachedChannel() {
        var h = new Harness().threeBrokers();
        h.world.partitionLeaders.put("t", 2);
        var client = h.client();
        produceOne(client, "t");

        client.close();

        assertThat(h.transports.values()).allMatch(t -> t.closed);
    }

    // ---- coordinator-routed calls ----

    @Test
    void coordinatorMovedRefindsAndRetriesTransparently() {
        var h = new Harness().threeBrokers();
        h.world.coordinatorId = 2;
        var client = h.client();
        var req = ConsumerGroupHeartbeatRequest.newBuilder().setGroupId("g").build();

        var first = client.consumerGroupHeartbeat(req, 30_000);
        assertThat(first.getError()).isEqualTo(ErrorCode.OK);

        // Coordinator moves to broker 3; broker 2 answers NOT_COORDINATOR
        // (world default once the id changed).
        h.world.coordinatorId = 3;
        int mark = h.world.calls.size();
        var second = client.consumerGroupHeartbeat(req, 30_000);

        assertThat(second.getError()).isEqualTo(ErrorCode.OK);
        var since = h.world.callsSince(mark);
        assertThat(since.get(0)).isEqualTo("2:heartbeat");
        assertThat(since).anyMatch(c -> c.endsWith(":findCoordinator"));
        assertThat(since.get(since.size() - 1)).isEqualTo("3:heartbeat");
    }

    @Test
    void deadCoordinatorIsReplacedOnTransportFailure() {
        var h = new Harness().threeBrokers();
        h.world.coordinatorId = 2;
        var client = h.client();
        var req = ConsumerGroupHeartbeatRequest.newBuilder().setGroupId("g").build();
        client.consumerGroupHeartbeat(req, 30_000);

        h.world.down.add(2);
        h.world.coordinatorId = 3;

        var resp = client.consumerGroupHeartbeat(req, 30_000);
        assertThat(resp.getError()).isEqualTo(ErrorCode.OK);
        assertThat(h.world.calls).contains("3:heartbeat");
    }
}
