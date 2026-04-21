package jbroker.broker.replication;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.proto.broker.ReplicaFetchRequest;
import jbroker.proto.broker.ReplicaFetchResponse;
import jbroker.storage.LogManager;
import jbroker.storage.LogSegment;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplicaFetcherTest {

    @Test
    void pollAppendsRecordsReceivedFromLeader(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var peer = new StubPeer();
            peer.enqueue(
                    encodedBatch(0L, List.of(valueOf("a"), valueOf("b"), valueOf("c"))), /* hwm */ 3L, /* epoch */ 0);

            var fetcher = new ReplicaFetcher(lm, "orders", 0, /* selfBrokerId */ 2, peer);
            fetcher.pollOnce(0);

            var log = lm.logFor("orders", 0);
            assertThat(log.nextOffset()).isEqualTo(3L);
            assertThat(fetcher.highWatermark()).isEqualTo(3L);
        }
    }

    @Test
    void pollIsNoOpWhenLeaderReturnsEmpty(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var peer = new StubPeer();
            peer.enqueue(ByteString.EMPTY, 0L, 0);

            var fetcher = new ReplicaFetcher(lm, "orders", 0, 2, peer);
            fetcher.pollOnce(0);

            assertThat(lm.logFor("orders", 0).nextOffset()).isZero();
        }
    }

    @Test
    void pollOnFencedEpochLeavesLogUnchanged(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var peer = new StubPeer();
            peer.enqueueError(jbroker.broker.ErrorCodes.FENCED_EPOCH, /* current epoch */ 5);

            var fetcher = new ReplicaFetcher(lm, "orders", 0, 2, peer);
            fetcher.pollOnce(/* my epoch */ 3);

            assertThat(lm.logFor("orders", 0).nextOffset()).isZero();
        }
    }

    // --- helpers ---

    private static byte[] valueOf(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static ByteString encodedBatch(long baseOffset, List<byte[]> values) {
        var records = new java.util.ArrayList<Record>();
        for (int i = 0; i < values.size(); i++) {
            records.add(new Record(i, 0L, null, values.get(i)));
        }
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        long now = 1_000L;
        RecordBatch.encode(buf, baseOffset, 0, now, now, -1L, (short) -1, -1, records);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return ByteString.copyFrom(bytes);
    }

    private static LogManager lm(Path dir) throws Exception {
        return new LogManager(
                dir,
                new LogManager.Config(
                        128L * 1024 * 1024,
                        Long.MAX_VALUE,
                        LogSegment.DEFAULT_INDEX_INTERVAL_BYTES,
                        TimeUnit.MINUTES.toMillis(5)));
    }

    /** Queue of canned responses for the fetcher's gRPC peer. */
    private static final class StubPeer implements ReplicaFetcher.Peer {
        private final java.util.Deque<ReplicaFetchResponse> queue = new java.util.ArrayDeque<>();

        void enqueue(ByteString records, long hwm, int currentEpoch) {
            queue.add(ReplicaFetchResponse.newBuilder()
                    .setRecords(records)
                    .setHighWatermark(hwm)
                    .setCurrentLeaderEpoch(currentEpoch)
                    .build());
        }

        void enqueueError(int code, int currentEpoch) {
            queue.add(ReplicaFetchResponse.newBuilder()
                    .setCurrentLeaderEpoch(currentEpoch)
                    .setError(jbroker.proto.broker.Error.newBuilder()
                            .setCode(code)
                            .setMessage("x")
                            .build())
                    .build());
        }

        @Override
        public ReplicaFetchResponse fetch(ReplicaFetchRequest req) {
            var resp = queue.poll();
            if (resp == null) throw new IllegalStateException("no canned response");
            return resp;
        }
    }
}
