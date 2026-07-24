package jbroker.broker.client.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import jbroker.proto.broker.Assignment;
import jbroker.proto.broker.CommitOffsetsRequest;
import jbroker.proto.broker.CommitOffsetsResponse;
import jbroker.proto.broker.ConsumerGroupHeartbeatRequest;
import jbroker.proto.broker.ConsumerGroupHeartbeatResponse;
import jbroker.proto.broker.FetchOffsetsRequest;
import jbroker.proto.broker.FetchOffsetsResponse;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.broker.FetchResponse;
import jbroker.proto.broker.ListOffsetsRequest;
import jbroker.proto.broker.ListOffsetsResponse;
import jbroker.proto.broker.OffsetFetchResult;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ProduceResponse;
import jbroker.proto.broker.TopicPartitions;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.Test;

/**
 * Consumer behavior at the {@link ConsumerRpc} seam — the contract the
 * cluster-routed path relies on: a {@code null} heartbeat or fetch is a
 * silent skip-this-tick (never an application-visible error), and a
 * rejoin after member eviction resumes at the live local position instead
 * of rewinding to a stale committed offset.
 */
class ConsumerClusterPathTest {

    private static final TopicPartition T0 =
            TopicPartition.newBuilder().setTopic("t").setPartition(0).build();

    /** Scripted seam: queued responses per call; empty queue = steady-state defaults. */
    private static final class FakeRpc implements ConsumerRpc {
        final ArrayDeque<ConsumerGroupHeartbeatResponse> heartbeats = new ArrayDeque<>();
        final ArrayDeque<FetchResponse> fetches = new ArrayDeque<>();
        final ArrayDeque<FetchOffsetsResponse> offsetFetches = new ArrayDeque<>();
        final List<FetchRequest> fetchRequests = new ArrayList<>();
        boolean closed;

        @Override
        public ConsumerGroupHeartbeatResponse heartbeat(ConsumerGroupHeartbeatRequest req) {
            return heartbeats.poll(); // empty queue = coordinator unreachable
        }

        @Override
        public void invalidateCoordinator() {}

        @Override
        public CommitOffsetsResponse commitOffsets(CommitOffsetsRequest req) {
            return CommitOffsetsResponse.getDefaultInstance();
        }

        @Override
        public FetchOffsetsResponse fetchOffsets(FetchOffsetsRequest req) {
            var scripted = offsetFetches.poll();
            if (scripted != null) return scripted;
            return FetchOffsetsResponse.newBuilder()
                    .addResults(OffsetFetchResult.newBuilder()
                            .setTp(T0)
                            .setOffset(-1)
                            .setError(ErrorCode.OK))
                    .build();
        }

        @Override
        public ListOffsetsResponse listOffsets(ListOffsetsRequest req) {
            return ListOffsetsResponse.getDefaultInstance();
        }

        @Override
        public FetchResponse fetch(FetchRequest req) {
            fetchRequests.add(req);
            return fetches.poll(); // empty queue = partition unreachable
        }

        @Override
        public ProduceResponse produceDlt(ProduceRequest req) {
            return ProduceResponse.getDefaultInstance();
        }

        @Override
        public void leaveGroup(ConsumerGroupHeartbeatRequest req) {}

        @Override
        public void close() {
            closed = true;
        }
    }

    private static ConsumerGroupHeartbeatResponse joined(String memberId, int epoch) {
        return ConsumerGroupHeartbeatResponse.newBuilder()
                .setError(ErrorCode.OK)
                .setMemberId(memberId)
                .setMemberEpoch(epoch)
                .setAssignment(Assignment.newBuilder()
                        .addAssignedPartitions(
                                TopicPartitions.newBuilder().setTopic("t").addPartitions(0)))
                .build();
    }

    private static ConsumerGroupHeartbeatResponse steady(String memberId, int epoch) {
        return ConsumerGroupHeartbeatResponse.newBuilder()
                .setError(ErrorCode.OK)
                .setMemberId(memberId)
                .setMemberEpoch(epoch)
                .build();
    }

    private static FetchResponse records(long baseOffset, String... values) {
        var records = new ArrayList<Record>(values.length);
        for (int i = 0; i < values.length; i++) {
            records.add(new Record(i, 0L, null, values[i].getBytes(StandardCharsets.UTF_8)));
        }
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        long now = System.currentTimeMillis();
        RecordBatch.encode(buf, baseOffset, 0, now, now, -1L, (short) -1, -1, records);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return FetchResponse.newBuilder().setRecords(ByteString.copyFrom(bytes)).build();
    }

    private static Consumer<String, String> consumer(FakeRpc rpc) {
        var cfg = ConsumerConfig.builder("g")
                .pollFetchDeadline(Duration.ofMillis(200))
                .build();
        var c = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer(), (ConsumerRpc) rpc);
        c.subscribe(List.of("t"), null);
        return c;
    }

    @Test
    void unreachableCoordinatorTickReturnsEmptyInsteadOfThrowing() {
        var rpc = new FakeRpc(); // heartbeat queue empty = unreachable
        try (var consumer = consumer(rpc)) {
            assertThat(consumer.poll(Duration.ofMillis(10)).isEmpty()).isTrue();
        }
    }

    @Test
    void unreachablePartitionTickSkipsFetchQuietly() {
        var rpc = new FakeRpc();
        rpc.heartbeats.add(joined("m1", 1)); // fetch queue empty = unreachable
        try (var consumer = consumer(rpc)) {
            assertThat(consumer.poll(Duration.ofMillis(10)).isEmpty()).isTrue();
        }
    }

    @Test
    void rejoinAfterMemberEvictionResumesAtLocalPositionNotStaleCommit() {
        var rpc = new FakeRpc();
        // Tick 1: join (nothing committed yet) and consume records 0..4 —
        // the local position advances to 5.
        rpc.heartbeats.add(joined("m1", 1));
        rpc.offsetFetches.add(FetchOffsetsResponse.newBuilder()
                .addResults(
                        OffsetFetchResult.newBuilder().setTp(T0).setOffset(-1).setError(ErrorCode.OK))
                .build());
        rpc.fetches.add(records(0, "a", "b", "c", "d", "e"));
        // Tick 2: coordinator failed over and evicted us.
        rpc.heartbeats.add(ConsumerGroupHeartbeatResponse.newBuilder()
                .setError(ErrorCode.UNKNOWN_MEMBER_ID)
                .build());
        // Tick 3: rejoin; the coordinator only saw a commit at offset 2.
        rpc.heartbeats.add(joined("m2", 1));
        rpc.offsetFetches.add(FetchOffsetsResponse.newBuilder()
                .addResults(
                        OffsetFetchResult.newBuilder().setTp(T0).setOffset(2).setError(ErrorCode.OK))
                .build());
        rpc.fetches.add(records(5, "f"));
        // Tick 4: steady state, another fetch.
        rpc.heartbeats.add(steady("m2", 1));
        rpc.fetches.add(FetchResponse.getDefaultInstance());

        try (var consumer = consumer(rpc)) {
            var first = consumer.poll(Duration.ofMillis(10));
            assertThat(first.count()).isEqualTo(5);
            assertThat(consumer.poll(Duration.ofMillis(10)).isEmpty()).isTrue(); // eviction tick

            var afterRejoin = consumer.poll(Duration.ofMillis(10));
            assertThat(afterRejoin.all().stream().map(ConsumerRecord::value))
                    .as("no record is re-delivered after the rejoin")
                    .containsExactly("f");
            consumer.poll(Duration.ofMillis(10));
        }

        assertThat(rpc.fetchRequests)
                .as("every fetch resumes from the live local position, never the stale commit")
                .extracting(FetchRequest::getOffset)
                .containsExactly(0L, 5L, 6L);
    }
}
