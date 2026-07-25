package jbroker.broker.client.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import jbroker.proto.broker.AbortedTxn;
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
import jbroker.storage.Compression;
import jbroker.storage.ControlRecord;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.Test;

/**
 * Client-side transactional filtering at the {@link ConsumerRpc} seam:
 * control batches are never surfaced under either isolation level, and
 * under read_committed the response's aborted ranges drop transactional
 * records from first_offset up to the producer's ABORT marker — a later
 * committed transaction of the same producer passes through.
 */
final class ConsumerReadCommittedTest {

    private static final TopicPartition T0 =
            TopicPartition.newBuilder().setTopic("t").setPartition(0).build();
    private static final long PID = 7L;
    private static final short EPOCH = 1;

    /** Scripted seam: queued responses per call; empty queue = steady-state defaults. */
    private static final class FakeRpc implements ConsumerRpc {
        final ArrayDeque<ConsumerGroupHeartbeatResponse> heartbeats = new ArrayDeque<>();
        final ArrayDeque<FetchResponse> fetches = new ArrayDeque<>();
        final List<FetchRequest> fetchRequests = new ArrayList<>();

        @Override
        public ConsumerGroupHeartbeatResponse heartbeat(ConsumerGroupHeartbeatRequest req) {
            return heartbeats.poll();
        }

        @Override
        public void invalidateCoordinator() {}

        @Override
        public CommitOffsetsResponse commitOffsets(CommitOffsetsRequest req) {
            return CommitOffsetsResponse.getDefaultInstance();
        }

        @Override
        public FetchOffsetsResponse fetchOffsets(FetchOffsetsRequest req) {
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
            return fetches.poll();
        }

        @Override
        public ProduceResponse produceDlt(ProduceRequest req) {
            return ProduceResponse.getDefaultInstance();
        }

        @Override
        public void leaveGroup(ConsumerGroupHeartbeatRequest req) {}

        @Override
        public void close() {}
    }

    @Test
    void controlBatchesAreNeverSurfacedEvenReadUncommitted() {
        var rpc = new FakeRpc();
        rpc.heartbeats.add(joined());
        var stream = new BatchStream()
                .plain(0, "a") // 0
                .marker(1, ControlRecord.Type.COMMIT) // 1
                .plain(2, "b"); // 2
        rpc.fetches.add(
                FetchResponse.newBuilder().setRecords(stream.byteString()).build());

        try (var consumer = consumer(rpc, ConsumerConfig.IsolationLevel.READ_UNCOMMITTED)) {
            var records = consumer.poll(Duration.ofMillis(10));
            assertThat(values(records)).containsExactly("a", "b");
            assertThat(offsets(records)).containsExactly(0L, 2L);
        }
    }

    @Test
    void readCommittedDropsAbortedRangesUsingTheAttachedList() {
        var rpc = new FakeRpc();
        rpc.heartbeats.add(joined());
        var stream = new BatchStream()
                .transactional(0, "t1", "t2") // 0..1, aborted
                .marker(2, ControlRecord.Type.ABORT) // 2
                .plain(3, "a"); // 3
        rpc.fetches.add(FetchResponse.newBuilder()
                .setRecords(stream.byteString())
                .setLastStableOffset(4)
                .addAbortedTxns(abortedTxn(PID, 0))
                .build());

        try (var consumer = consumer(rpc, ConsumerConfig.IsolationLevel.READ_COMMITTED)) {
            var records = consumer.poll(Duration.ofMillis(10));
            assertThat(values(records)).containsExactly("a");
            assertThat(offsets(records)).containsExactly(3L);
        }
    }

    @Test
    void abortMarkerClosesTheRangeSoALaterCommittedTxnPasses() {
        var rpc = new FakeRpc();
        rpc.heartbeats.add(joined());
        var stream = new BatchStream()
                .transactional(0, "bad1", "bad2") // 0..1, aborted
                .marker(2, ControlRecord.Type.ABORT) // 2
                .transactional(3, "good1", "good2") // 3..4, committed
                .marker(5, ControlRecord.Type.COMMIT) // 5
                .plain(6, "plain"); // 6
        rpc.fetches.add(FetchResponse.newBuilder()
                .setRecords(stream.byteString())
                .setLastStableOffset(7)
                .addAbortedTxns(abortedTxn(PID, 0))
                .build());

        try (var consumer = consumer(rpc, ConsumerConfig.IsolationLevel.READ_COMMITTED)) {
            var records = consumer.poll(Duration.ofMillis(10));
            assertThat(values(records)).containsExactly("good1", "good2", "plain");
            assertThat(offsets(records)).containsExactly(3L, 4L, 6L);
        }
    }

    @Test
    void nonTransactionalRecordsInterleavedWithAnAbortedTxnSurvive() {
        var rpc = new FakeRpc();
        rpc.heartbeats.add(joined());
        var stream = new BatchStream()
                .transactional(0, "bad") // 0, aborted
                .plain(1, "keep") // 1, a different producer's plain data
                .marker(2, ControlRecord.Type.ABORT); // 2
        rpc.fetches.add(FetchResponse.newBuilder()
                .setRecords(stream.byteString())
                .setLastStableOffset(3)
                .addAbortedTxns(abortedTxn(PID, 0))
                .build());

        try (var consumer = consumer(rpc, ConsumerConfig.IsolationLevel.READ_COMMITTED)) {
            var records = consumer.poll(Duration.ofMillis(10));
            assertThat(values(records)).containsExactly("keep");
        }
    }

    @Test
    void abortedListIsIgnoredUnderReadUncommitted() {
        var rpc = new FakeRpc();
        rpc.heartbeats.add(joined());
        var stream = new BatchStream()
                .transactional(0, "t1") // 0
                .marker(1, ControlRecord.Type.ABORT); // 1
        rpc.fetches.add(FetchResponse.newBuilder()
                .setRecords(stream.byteString())
                .addAbortedTxns(abortedTxn(PID, 0))
                .build());

        try (var consumer = consumer(rpc, ConsumerConfig.IsolationLevel.READ_UNCOMMITTED)) {
            var records = consumer.poll(Duration.ofMillis(10));
            assertThat(values(records)).containsExactly("t1");
        }
    }

    @Test
    void fetchRequestCarriesTheConfiguredIsolationLevel() {
        var rpc = new FakeRpc();
        rpc.heartbeats.add(joined());
        rpc.fetches.add(FetchResponse.getDefaultInstance());
        try (var consumer = consumer(rpc, ConsumerConfig.IsolationLevel.READ_COMMITTED)) {
            consumer.poll(Duration.ofMillis(10));
        }
        assertThat(rpc.fetchRequests).hasSize(1);
        assertThat(rpc.fetchRequests.get(0).getIsolationLevel()).isEqualTo(1);

        var rpcDefault = new FakeRpc();
        rpcDefault.heartbeats.add(joined());
        rpcDefault.fetches.add(FetchResponse.getDefaultInstance());
        try (var consumer = consumer(rpcDefault, ConsumerConfig.IsolationLevel.READ_UNCOMMITTED)) {
            consumer.poll(Duration.ofMillis(10));
        }
        assertThat(rpcDefault.fetchRequests.get(0).getIsolationLevel()).isZero();
    }

    // ---------- helpers ----------

    /** Builds a byte stream of consecutive v2 batches like a broker fetch response carries. */
    private static final class BatchStream {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        BatchStream plain(long baseOffset, String... values) {
            return append(baseOffset, -1L, (short) -1, false, values);
        }

        BatchStream transactional(long baseOffset, String... values) {
            return append(baseOffset, PID, EPOCH, true, values);
        }

        BatchStream marker(long offset, ControlRecord.Type type) {
            var buf = ByteBuffer.allocate(256);
            RecordBatch.encodeControl(buf, offset, 0, 0L, PID, EPOCH, new ControlRecord(type, 1));
            flush(buf);
            return this;
        }

        private BatchStream append(
                long baseOffset, long producerId, short producerEpoch, boolean transactional, String... values) {
            var records = new ArrayList<Record>(values.length);
            for (int i = 0; i < values.length; i++) {
                records.add(new Record(i, 0L, null, values[i].getBytes(StandardCharsets.UTF_8)));
            }
            var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
            RecordBatch.encode(
                    buf,
                    baseOffset,
                    0,
                    0L,
                    0L,
                    producerId,
                    producerEpoch,
                    -1,
                    records,
                    Compression.NONE,
                    transactional);
            flush(buf);
            return this;
        }

        private void flush(ByteBuffer buf) {
            buf.flip();
            byte[] out = new byte[buf.remaining()];
            buf.get(out);
            bytes.writeBytes(out);
        }

        ByteString byteString() {
            return ByteString.copyFrom(bytes.toByteArray());
        }
    }

    private static AbortedTxn abortedTxn(long producerId, long firstOffset) {
        return AbortedTxn.newBuilder()
                .setProducerId(producerId)
                .setFirstOffset(firstOffset)
                .build();
    }

    private static ConsumerGroupHeartbeatResponse joined() {
        return ConsumerGroupHeartbeatResponse.newBuilder()
                .setError(ErrorCode.OK)
                .setMemberId("m1")
                .setMemberEpoch(1)
                .setAssignment(Assignment.newBuilder()
                        .addAssignedPartitions(
                                TopicPartitions.newBuilder().setTopic("t").addPartitions(0)))
                .build();
    }

    private static Consumer<String, String> consumer(FakeRpc rpc, ConsumerConfig.IsolationLevel isolation) {
        var cfg = ConsumerConfig.builder("g")
                .pollFetchDeadline(Duration.ofMillis(200))
                .isolationLevel(isolation)
                .build();
        var c = new Consumer<>(cfg, new StringDeserializer(), new StringDeserializer(), (ConsumerRpc) rpc);
        c.subscribe(List.of("t"), null);
        return c;
    }

    private static List<String> values(ConsumerRecords<String, String> records) {
        var out = new ArrayList<String>();
        for (var r : records) out.add(r.value());
        return out;
    }

    private static List<Long> offsets(ConsumerRecords<String, String> records) {
        var out = new ArrayList<Long>();
        for (var r : records) out.add(r.offset());
        return out;
    }
}
