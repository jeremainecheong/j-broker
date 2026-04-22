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
            assertThat(fetcher.pollOnce(0)).isEqualTo(ReplicaFetcher.PollResult.ADVANCED);

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
            assertThat(fetcher.pollOnce(0)).isEqualTo(ReplicaFetcher.PollResult.EMPTY);

            assertThat(lm.logFor("orders", 0).nextOffset()).isZero();
        }
    }

    @Test
    void multiPollCatchupDoesNotDoubleAppendStaleBatches(@TempDir Path dir) throws Exception {
        // Repro for reviewer's issue #1: the leader's transferTo streams from
        // the sparse-index floor, so a follower whose fetchOffset lands *after*
        // an index entry will receive batches that straddle or precede it.
        // The fetcher must skip any batch whose last offset is below fetchOffset.
        try (var lm = lm(dir)) {
            var peer = new StubPeer();
            // Poll 1: leader sends a batch covering offsets 0..2.
            peer.enqueue(encodedBatch(0L, List.of(valueOf("a"), valueOf("b"), valueOf("c"))), 5L, 0);
            // Poll 2: follower's fetchOffset is now 3, but the leader's sparse
            // index re-sends the 0..2 batch (simulating the floor-lookup
            // behaviour) followed by the 3..4 batch. The fetcher must skip the
            // first and only append the second.
            var combined = concat(
                    encodedBatch(0L, List.of(valueOf("a"), valueOf("b"), valueOf("c"))),
                    encodedBatch(3L, List.of(valueOf("d"), valueOf("e"))));
            peer.enqueue(combined, 5L, 0);

            var fetcher = new ReplicaFetcher(lm, "orders", 0, 2, peer);
            fetcher.pollOnce(0);
            assertThat(lm.logFor("orders", 0).nextOffset()).isEqualTo(3L);
            fetcher.pollOnce(0);
            assertThat(lm.logFor("orders", 0).nextOffset()).isEqualTo(5L);
        }
    }

    @Test
    void pollOnFencedEpochReconcilesViaOffsetsForLeaderEpoch(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            // Follower has 5 records under epoch 0 — 3 that the leader
            // agrees with (0..2) and 2 divergent ones (3..4) it wrote
            // before a leadership change.
            var log = lm.logFor("orders", 0);
            for (int i = 0; i < 5; i++) {
                log.append(List.of(new jbroker.storage.Record(0, 0L, null, new byte[] {(byte) i})), 1_000L);
            }
            var peer = new StubPeer();
            // First fetch: FENCED, leader is at epoch 1 now.
            peer.enqueueError(jbroker.broker.ErrorCodes.FENCED_EPOCH, /* current epoch */ 1);
            // Follower asks OffsetsForLeaderEpoch(epoch=0) -> leader says
            // epoch 0 ended at offset 3.
            peer.enqueueOffsets(3L);

            var fetcher = new ReplicaFetcher(lm, "orders", 0, 2, peer);
            var result = fetcher.pollOnce(/* follower's last epoch */ 0);

            assertThat(result).isEqualTo(ReplicaFetcher.PollResult.RECONCILED);
            // Divergent tail (offsets 3..4) truncated.
            assertThat(lm.logFor("orders", 0).nextOffset()).isEqualTo(3L);
        }
    }

    @Test
    void pollOnFencedEpochTruncatesToZeroWhenLeaderReturnsUndefinedEpochOffset(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            // Follower has 4 records it wrote under an epoch that predates
            // anything the leader remembers (e.g. leader's checkpoint was
            // truncated by retention, or the follower is restoring from a
            // very old snapshot).
            var log = lm.logFor("orders", 0);
            for (int i = 0; i < 4; i++) {
                log.append(List.of(new jbroker.storage.Record(0, 0L, null, new byte[] {(byte) i})), 1_000L);
            }
            var peer = new StubPeer();
            peer.enqueueError(jbroker.broker.ErrorCodes.FENCED_EPOCH, /* current epoch */ 7);
            // Leader replies with UNDEFINED_EPOCH_OFFSET.
            peer.enqueueOffsets(jbroker.broker.OffsetsForLeaderEpochHandler.UNDEFINED_EPOCH_OFFSET);

            var fetcher = new ReplicaFetcher(lm, "orders", 0, 2, peer);
            var result = fetcher.pollOnce(/* follower's stale epoch */ 2);

            assertThat(result).isEqualTo(ReplicaFetcher.PollResult.RECONCILED);
            // Entire divergent log dropped — rebuild from offset 0.
            assertThat(lm.logFor("orders", 0).nextOffset()).isZero();
        }
    }

    @Test
    void pollUsesAppendRawToPreserveLeaderBatchMetadata(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var peer = new StubPeer();
            long leaderTs = 1_700_000_000_000L;
            long producerId = 99L;
            short producerEpoch = 7;
            int baseSeq = 42;
            int partitionLeaderEpoch = 4;
            peer.enqueue(
                    encodedBatchRich(
                            0L,
                            List.of(valueOf("a"), valueOf("b")),
                            leaderTs,
                            producerId,
                            producerEpoch,
                            baseSeq,
                            partitionLeaderEpoch),
                    2L,
                    partitionLeaderEpoch);

            var fetcher = new ReplicaFetcher(lm, "orders", 0, 2, peer);
            fetcher.pollOnce(partitionLeaderEpoch);

            // Read the follower's log back and verify metadata is preserved.
            var batches = lm.logFor("orders", 0).read(0L, 1024);
            assertThat(batches).hasSize(1);
            var parsed = batches.get(0);
            assertThat(parsed.firstTimestamp()).isEqualTo(leaderTs);
            assertThat(parsed.producerId()).isEqualTo(producerId);
            assertThat(parsed.producerEpoch()).isEqualTo(producerEpoch);
            assertThat(parsed.baseSequence()).isEqualTo(baseSeq);
            assertThat(parsed.partitionLeaderEpoch()).isEqualTo(partitionLeaderEpoch);
        }
    }

    @Test
    void pollOnArbitraryErrorReturnsErrorResult(@TempDir Path dir) throws Exception {
        try (var lm = lm(dir)) {
            var peer = new StubPeer();
            peer.enqueueError(jbroker.broker.ErrorCodes.IO_ERROR, 0);

            var fetcher = new ReplicaFetcher(lm, "orders", 0, 2, peer);
            assertThat(fetcher.pollOnce(0)).isEqualTo(ReplicaFetcher.PollResult.ERROR);
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

    private static ByteString encodedBatchRich(
            long baseOffset,
            List<byte[]> values,
            long firstTimestamp,
            long producerId,
            short producerEpoch,
            int baseSeq,
            int partitionLeaderEpoch) {
        var records = new java.util.ArrayList<Record>();
        for (int i = 0; i < values.size(); i++) {
            records.add(new Record(i, 0L, null, values.get(i)));
        }
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        RecordBatch.encode(
                buf,
                baseOffset,
                partitionLeaderEpoch,
                firstTimestamp,
                firstTimestamp,
                producerId,
                producerEpoch,
                baseSeq,
                records);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return ByteString.copyFrom(bytes);
    }

    private static ByteString concat(ByteString... parts) {
        var out = ByteString.EMPTY;
        for (var p : parts) out = out.concat(p);
        return out;
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

        private final java.util.Deque<Long> offsetResponses = new java.util.ArrayDeque<>();

        @Override
        public ReplicaFetchResponse fetch(ReplicaFetchRequest req) {
            var resp = queue.poll();
            if (resp == null) throw new IllegalStateException("no canned response");
            return resp;
        }

        void enqueueOffsets(long endOffset) {
            offsetResponses.add(endOffset);
        }

        @Override
        public long offsetsForLeaderEpoch(String topic, int partition, int leaderEpoch) {
            var v = offsetResponses.poll();
            if (v == null) throw new IllegalStateException("no canned offsetsForLeaderEpoch");
            return v;
        }
    }
}
