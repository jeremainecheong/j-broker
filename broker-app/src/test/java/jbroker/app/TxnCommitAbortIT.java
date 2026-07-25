package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.client.BrokerClient;
import jbroker.broker.txn.TxnStateTopic;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ProducerGrpc;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.common.TopicPartition;
import jbroker.proto.txn.AddPartitionsToTxnRequest;
import jbroker.proto.txn.EndTxnRequest;
import jbroker.proto.txn.InitTransactionsRequest;
import jbroker.proto.txn.TxnGrpc;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import jbroker.storage.Compression;
import jbroker.storage.ControlRecord;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Transactions against a live 3-broker cluster. First flow: a
 * transactional producer inits, registers two partitions, produces to
 * both, commits — commit control batches land in both partition logs and
 * a plain consumer sees the data. Second flow: the next transaction
 * aborts — the aborted range is recorded in the storage-level aborted
 * index, and a read_uncommitted consumer still sees the aborted data
 * (read_committed fetch filtering is the next slice; the storage query is
 * the contract it will consume). Third flow, own cluster: the coordinator
 * broker is killed right after EndTxn returns — the successor coordinator
 * replays {@code __transaction_state}, resumes the pending markers, and
 * the commit completes.
 */
class TxnCommitAbortIT {

    // Shared CI runners replicate and elect several times slower than a
    // laptop; scale every wall-clock wait like the sibling lifecycle ITs.
    private static final int CI_MULT =
            "1".equals(System.getenv("JBROKER_CI")) || "true".equalsIgnoreCase(System.getenv("CI")) ? 4 : 1;

    @Test
    void commitLandsMarkersAndAbortRecordsAbortedRange(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(
                3,
                2,
                (i, voters, ports) ->
                        new Broker.Config(new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters))) {
            awaitClusterReady(cluster);
            createTopic(cluster, "txn-data", 2);

            String txnId = "it-txn";
            int coordIdx = awaitCoordinator(cluster, txnId);
            try (var channels = new Channels()) {
                int coordPort = cluster.brokerPort(coordIdx);

                // --- transaction 1: commit across two partitions ---
                var init = channels.txnStub(coordPort)
                        .initTransactions(InitTransactionsRequest.newBuilder()
                                .setTransactionalId(txnId)
                                .build());
                assertThat(init.getError()).isEqualTo(ErrorCode.OK);
                long pid = init.getProducerId();
                int epoch = init.getProducerEpoch();
                assertThat(pid).isPositive();
                assertThat(epoch).isZero();

                var add = channels.txnStub(coordPort)
                        .addPartitionsToTxn(AddPartitionsToTxnRequest.newBuilder()
                                .setTransactionalId(txnId)
                                .setProducerId(pid)
                                .setProducerEpoch(epoch)
                                .addPartitions(tp("txn-data", 0))
                                .addPartitions(tp("txn-data", 1))
                                .build());
                assertThat(add.getError()).isEqualTo(ErrorCode.OK);

                produceTxn(cluster, channels, "txn-data", 0, pid, epoch, 0, "committed-0");
                produceTxn(cluster, channels, "txn-data", 1, pid, epoch, 0, "committed-1");

                var commit = channels.txnStub(coordPort)
                        .endTxn(EndTxnRequest.newBuilder()
                                .setTransactionalId(txnId)
                                .setProducerId(pid)
                                .setProducerEpoch(epoch)
                                .setCommit(true)
                                .build());
                assertThat(commit.getError()).isEqualTo(ErrorCode.OK);

                awaitMarker(cluster, "txn-data", 0, pid, ControlRecord.Type.COMMIT);
                awaitMarker(cluster, "txn-data", 1, pid, ControlRecord.Type.COMMIT);

                // A plain consumer sees the committed data.
                for (int p = 0; p < 2; p++) {
                    final String expected = "committed-" + p;
                    int leaderIdx = leaderIndexOf(cluster, "txn-data", p);
                    try (var client = new BrokerClient("127.0.0.1", cluster.brokerPort(leaderIdx))) {
                        var values = client.fetchAll("txn-data", p, 1 << 20);
                        assertThat(values)
                                .as("committed record visible on txn-data-%d", p)
                                .anySatisfy(v -> assertThat(new String(v, StandardCharsets.UTF_8))
                                        .isEqualTo(expected));
                    }
                }

                // --- transaction 2: abort on partition 0 ---
                var init2 = channels.txnStub(coordPort)
                        .initTransactions(InitTransactionsRequest.newBuilder()
                                .setTransactionalId(txnId)
                                .build());
                assertThat(init2.getError()).isEqualTo(ErrorCode.OK);
                assertThat(init2.getProducerId()).isEqualTo(pid);
                int epoch2 = init2.getProducerEpoch();
                assertThat(epoch2).isEqualTo(epoch + 1);

                var add2 = channels.txnStub(coordPort)
                        .addPartitionsToTxn(AddPartitionsToTxnRequest.newBuilder()
                                .setTransactionalId(txnId)
                                .setProducerId(pid)
                                .setProducerEpoch(epoch2)
                                .addPartitions(tp("txn-data", 0))
                                .build());
                assertThat(add2.getError()).isEqualTo(ErrorCode.OK);

                produceTxn(cluster, channels, "txn-data", 0, pid, epoch2, 0, "aborted-0");

                var abort = channels.txnStub(coordPort)
                        .endTxn(EndTxnRequest.newBuilder()
                                .setTransactionalId(txnId)
                                .setProducerId(pid)
                                .setProducerEpoch(epoch2)
                                .setCommit(false)
                                .build());
                assertThat(abort.getError()).isEqualTo(ErrorCode.OK);

                awaitMarker(cluster, "txn-data", 0, pid, ControlRecord.Type.ABORT);

                // The aborted range is queryable at the storage layer —
                // the exact contract the read_committed fetch slice
                // consumes to filter and to build its aborted-txn response
                // field.
                int leaderIdx = leaderIndexOf(cluster, "txn-data", 0);
                var log = cluster.broker(leaderIdx).logManager().logFor("txn-data", 0);
                var aborted = log.abortedTxnsIn(0, log.nextOffset());
                assertThat(aborted).anySatisfy(range -> {
                    assertThat(range.producerId()).isEqualTo(pid);
                    assertThat(range.firstOffset()).isEqualTo(range.lastOffset()); // single aborted record
                });

                // Until read_committed lands, a plain (read_uncommitted)
                // consumer still sees the aborted record.
                try (var client = new BrokerClient("127.0.0.1", cluster.brokerPort(leaderIdx))) {
                    var values = client.fetchAll("txn-data", 0, 1 << 20);
                    assertThat(values).anySatisfy(v -> assertThat(new String(v, StandardCharsets.UTF_8))
                            .isEqualTo("aborted-0"));
                }
            }
        }
    }

    @Test
    void coordinatorFailoverAfterEndTxnStillCompletesTheCommit(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(
                3,
                2,
                (i, voters, ports) ->
                        new Broker.Config(new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters))) {
            awaitClusterReady(cluster);
            createTopic(cluster, "txn-fo", 2);

            String txnId = "it-txn-failover";
            int coordIdx = awaitCoordinator(cluster, txnId);
            try (var channels = new Channels()) {
                int coordPort = cluster.brokerPort(coordIdx);
                var init = channels.txnStub(coordPort)
                        .initTransactions(InitTransactionsRequest.newBuilder()
                                .setTransactionalId(txnId)
                                .build());
                assertThat(init.getError()).isEqualTo(ErrorCode.OK);
                long pid = init.getProducerId();
                int epoch = init.getProducerEpoch();

                var add = channels.txnStub(coordPort)
                        .addPartitionsToTxn(AddPartitionsToTxnRequest.newBuilder()
                                .setTransactionalId(txnId)
                                .setProducerId(pid)
                                .setProducerEpoch(epoch)
                                .addPartitions(tp("txn-fo", 0))
                                .addPartitions(tp("txn-fo", 1))
                                .build());
                assertThat(add.getError()).isEqualTo(ErrorCode.OK);

                produceTxn(cluster, channels, "txn-fo", 0, pid, epoch, 0, "fo-0");
                produceTxn(cluster, channels, "txn-fo", 1, pid, epoch, 0, "fo-1");

                // EndTxn answers once PrepareCommit is replicated — the
                // decision survives the coordinator.
                var commit = channels.txnStub(coordPort)
                        .endTxn(EndTxnRequest.newBuilder()
                                .setTransactionalId(txnId)
                                .setProducerId(pid)
                                .setProducerEpoch(epoch)
                                .setCommit(true)
                                .build());
                assertThat(commit.getError()).isEqualTo(ErrorCode.OK);

                // Kill the coordinator immediately. Marker delivery may or
                // may not have started; either way the commit must
                // complete — the successor replays PrepareCommit and
                // resumes the pending markers.
                cluster.broker(coordIdx).closeAbruptly();

                // Generous budget: failover needs fencing (a few seconds)
                // plus the ISR shrink (~12s lag timeout) before acks=all
                // marker appends can confirm on the survivors.
                awaitMarkerOnSurvivors(cluster, coordIdx, "txn-fo", 0, pid, ControlRecord.Type.COMMIT);
                awaitMarkerOnSurvivors(cluster, coordIdx, "txn-fo", 1, pid, ControlRecord.Type.COMMIT);
            }
        }
    }

    // --- cluster helpers ---

    private static void awaitClusterReady(TestBrokerCluster cluster) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            long leaders = cluster.brokers().stream()
                    .filter(b -> b.role() == Role.LEADER)
                    .count();
            if (leaders == 1) break;
            Thread.sleep(50);
        }
        assertThat(cluster.brokers().stream()
                        .filter(b -> b.role() == Role.LEADER)
                        .count())
                .as("single Raft leader")
                .isEqualTo(1);
        deadline = System.currentTimeMillis() + 10_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            boolean allKnowAll = cluster.brokers().stream()
                    .allMatch(b -> b.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3)));
            if (allKnowAll) return;
            Thread.sleep(50);
        }
        throw new AssertionError("broker registry did not converge");
    }

    private static Broker raftLeader(TestBrokerCluster cluster) {
        return cluster.brokers().stream()
                .filter(b -> b.role() == Role.LEADER)
                .findFirst()
                .orElseThrow();
    }

    private static void createTopic(TestBrokerCluster cluster, String topic, int partitions) throws Exception {
        try (var client = new BrokerClient("127.0.0.1", raftLeader(cluster).brokerPort())) {
            client.createTopic(topic, partitions, /*rf*/ 3);
        }
        long deadline = System.currentTimeMillis() + 10_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            boolean allLed = true;
            for (int p = 0; p < partitions; p++) {
                var leader = cluster.broker(0).topics().partitionLeader(topic, p);
                if (leader.isEmpty() || leader.get() <= 0) allLed = false;
            }
            if (allLed) return;
            Thread.sleep(50);
        }
        throw new AssertionError("topic " + topic + " never got leaders for all partitions");
    }

    /**
     * Wait for {@code __transaction_state} to exist with a live leader for
     * {@code txnId}'s coordinator partition; returns the coordinator's
     * cluster index.
     */
    private static int awaitCoordinator(TestBrokerCluster cluster, String txnId) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            var desc = cluster.broker(0).topics().describe(TxnStateTopic.NAME);
            if (desc.isPresent()) {
                int partition = TxnStateTopic.partitionFor(txnId, desc.get().partitions());
                var leader = cluster.broker(0).topics().partitionLeader(TxnStateTopic.NAME, partition);
                if (leader.isPresent() && leader.get() > 0) {
                    return leader.get() - 1; // node ids are 1-based cluster indices
                }
            }
            Thread.sleep(100);
        }
        throw new AssertionError("__transaction_state never became coordinatable for '" + txnId + "'");
    }

    private static int leaderIndexOf(TestBrokerCluster cluster, String topic, int partition) {
        // Read metadata from any live broker (index 1 survives both tests'
        // kill patterns only by luck — walk until one answers).
        for (var b : cluster.brokers()) {
            try {
                var leader = b.topics().partitionLeader(topic, partition);
                if (leader.isPresent() && leader.get() > 0) return leader.get() - 1;
            } catch (RuntimeException ignored) {
                // closed broker — try the next one
            }
        }
        throw new AssertionError("no live broker knows a leader for " + topic + "-" + partition);
    }

    // --- transactional produce ---

    private void produceTxn(
            TestBrokerCluster cluster,
            Channels channels,
            String topic,
            int partition,
            long pid,
            int epoch,
            int baseSeq,
            String value) {
        int leaderIdx = leaderIndexOf(cluster, topic, partition);
        var records = List.of(new Record(0, 0L, null, value.getBytes(StandardCharsets.UTF_8)));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        RecordBatch.encode(
                buf, 0L, 0, 1L, 1L, pid, (short) epoch, baseSeq, records, Compression.NONE, /*transactional*/ true);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        var resp = channels.producerStub(cluster.brokerPort(leaderIdx))
                .produce(ProduceRequest.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .setProducerId(pid)
                        .setProducerEpoch(epoch)
                        .setBaseSequence(baseSeq)
                        .setAcks(-1)
                        .setBatch(ByteString.copyFrom(bytes))
                        .build());
        assertThat(resp.hasError() && resp.getError().getCode() != 0)
                .as(
                        "transactional produce to %s-%d: %s",
                        topic, partition, resp.getError().getMessage())
                .isFalse();
    }

    // --- marker assertions ---

    private static void awaitMarker(
            TestBrokerCluster cluster, String topic, int partition, long pid, ControlRecord.Type type)
            throws Exception {
        long deadline = System.currentTimeMillis() + 15_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            int leaderIdx = leaderIndexOf(cluster, topic, partition);
            if (hasMarker(cluster.broker(leaderIdx), topic, partition, pid, type)) return;
            Thread.sleep(100);
        }
        throw new AssertionError("no " + type + " marker for pid " + pid + " on " + topic + "-" + partition);
    }

    /** Post-kill form: only consults brokers other than {@code deadIdx}. */
    private static void awaitMarkerOnSurvivors(
            TestBrokerCluster cluster, int deadIdx, String topic, int partition, long pid, ControlRecord.Type type)
            throws Exception {
        long deadline = System.currentTimeMillis() + 60_000L * CI_MULT;
        while (System.currentTimeMillis() < deadline) {
            for (int i = 0; i < cluster.brokers().size(); i++) {
                if (i == deadIdx) continue;
                try {
                    if (hasMarker(cluster.broker(i), topic, partition, pid, type)) return;
                } catch (RuntimeException ignored) {
                    // broker mid-shutdown race; keep polling
                }
            }
            Thread.sleep(200);
        }
        throw new AssertionError(
                "commit never completed on " + topic + "-" + partition + " after coordinator failover");
    }

    private static boolean hasMarker(Broker broker, String topic, int partition, long pid, ControlRecord.Type type)
            throws Exception {
        var log = broker.logManager().logFor(topic, partition);
        long offset = 0;
        long end = log.nextOffset();
        while (offset < end) {
            var batches = log.read(offset, 1 << 20);
            if (batches.isEmpty()) break;
            for (var b : batches) {
                if (b.control() && b.producerId() == pid && b.controlRecord().type() == type) {
                    return true;
                }
                offset = b.lastOffset() + 1;
            }
        }
        return false;
    }

    private static TopicPartition tp(String topic, int partition) {
        return TopicPartition.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .build();
    }

    /** Per-port plaintext channel cache so stubs follow leadership wherever it lands. */
    private static final class Channels implements AutoCloseable {
        private final java.util.Map<Integer, ManagedChannel> byPort = new java.util.HashMap<>();

        private ManagedChannel channel(int port) {
            return byPort.computeIfAbsent(port, p -> ManagedChannelBuilder.forAddress("127.0.0.1", p)
                    .usePlaintext()
                    .build());
        }

        TxnGrpc.TxnBlockingStub txnStub(int port) {
            return TxnGrpc.newBlockingStub(channel(port)).withDeadlineAfter(30, TimeUnit.SECONDS);
        }

        ProducerGrpc.ProducerBlockingStub producerStub(int port) {
            return ProducerGrpc.newBlockingStub(channel(port)).withDeadlineAfter(30, TimeUnit.SECONDS);
        }

        @Override
        public void close() {
            byPort.values().forEach(ManagedChannel::shutdownNow);
        }
    }
}
