package jbroker.broker.client;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.proto.broker.AdminGrpc;
import jbroker.proto.broker.ConsumerGrpc;
import jbroker.proto.broker.CreateTopicRequest;
import jbroker.proto.broker.DeleteTopicRequest;
import jbroker.proto.broker.DescribeTopicRequest;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.broker.InitProducerIdRequest;
import jbroker.proto.broker.ListTopicsRequest;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ProducerGrpc;
import jbroker.proto.broker.TopicDescription;
import jbroker.proto.broker.UpdateTopicConfigRequest;
import jbroker.storage.Record;
import jbroker.storage.RecordBatch;
import jbroker.tls.TlsConfig;
import jbroker.tls.TlsContexts;

/**
 * Minimal single-broker client surface. Bundles Producer,
 * Consumer, and Admin stubs against one gRPC channel. No retries, no
 * batching — the simplest possible shape that lets the E2E test cover
 * "produce N, consume N" end-to-end.
 */
public final class BrokerClient implements AutoCloseable {

    private final ManagedChannel channel;
    private final ProducerGrpc.ProducerBlockingStub producer;
    private final ConsumerGrpc.ConsumerBlockingStub consumer;
    private final AdminGrpc.AdminBlockingStub admin;
    private final jbroker.proto.broker.MetadataGrpc.MetadataBlockingStub metadata;

    public BrokerClient(String host, int port) {
        this(host, port, TlsConfig.DISABLED);
    }

    /** TLS-aware constructor. Pass {@link TlsConfig#DISABLED} for plaintext. */
    public BrokerClient(String host, int port, TlsConfig tls) {
        var b = NettyChannelBuilder.forAddress(host, port);
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
        this.metadata = jbroker.proto.broker.MetadataGrpc.newBlockingStub(channel);
    }

    /** This broker's observability snapshot ({@code Metadata.DescribeMetrics}). */
    public jbroker.proto.broker.DescribeMetricsResponse describeMetrics() {
        return metadata.withDeadlineAfter(5, TimeUnit.SECONDS)
                .describeMetrics(jbroker.proto.broker.DescribeMetricsRequest.getDefaultInstance());
    }

    // ---- Admin ----

    public void createTopic(String topic, int partitions, int replicationFactor) {
        var resp = admin.withDeadlineAfter(10, TimeUnit.SECONDS)
                .createTopic(CreateTopicRequest.newBuilder()
                        .setTopic(topic)
                        .setPartitions(partitions)
                        .setReplicationFactor(replicationFactor)
                        .build());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException("createTopic failed: " + resp.getError().getMessage());
        }
    }

    public List<TopicDescription> listTopics() {
        var resp = admin.withDeadlineAfter(5, TimeUnit.SECONDS)
                .listTopics(ListTopicsRequest.newBuilder().build());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException("listTopics failed: " + resp.getError().getMessage());
        }
        return resp.getTopicsList();
    }

    public TopicDescription describeTopic(String topic) {
        var resp = admin.withDeadlineAfter(5, TimeUnit.SECONDS)
                .describeTopic(DescribeTopicRequest.newBuilder().setTopic(topic).build());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException(
                    "describeTopic failed: " + resp.getError().getMessage());
        }
        return resp.getTopic();
    }

    /** Delete a topic cluster-wide. Raises on NOT_LEADER / IO / UNKNOWN_TOPIC. */
    public void deleteTopic(String topic) {
        var resp = admin.withDeadlineAfter(5, TimeUnit.SECONDS)
                .deleteTopic(DeleteTopicRequest.newBuilder().setTopic(topic).build());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException("deleteTopic failed: " + resp.getError().getMessage());
        }
    }

    /** Merge a config overlay into an existing topic. Returns the merged map. */
    public java.util.Map<String, String> updateTopicConfig(String topic, java.util.Map<String, String> config) {
        var b = UpdateTopicConfigRequest.newBuilder().setTopic(topic);
        b.putAllConfig(config);
        var resp = admin.withDeadlineAfter(5, TimeUnit.SECONDS).updateTopicConfig(b.build());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException(
                    "updateTopicConfig failed: " + resp.getError().getMessage());
        }
        return resp.getConfigMap();
    }

    /**
     * Force synchronous compaction on the responding broker's local
     * log for {@code (topic, partition)}. Returns the survivor count, or
     * -1 if the broker has no open log for that partition. Throws on any
     * populated {@code error} (unknown topic, invalid partition, I/O error).
     */
    public int forceCompactPartition(String topic, int partition) {
        var resp = admin.withDeadlineAfter(10, TimeUnit.SECONDS)
                .forceCompactPartition(jbroker.proto.broker.ForceCompactPartitionRequest.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .build());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException(
                    "forceCompactPartition failed: " + resp.getError().getMessage());
        }
        return resp.getRecordsKept();
    }

    /** Create (or overwrite) an ACL entry cluster-wide. */
    public void createAcl(
            String principal,
            String resourceType,
            String resourceName,
            boolean prefix,
            String operation,
            boolean allow) {
        var resp = admin.withDeadlineAfter(10, TimeUnit.SECONDS)
                .createAcl(jbroker.proto.broker.CreateAclRequest.newBuilder()
                        .setEntry(aclEntry(principal, resourceType, resourceName, prefix, operation, allow))
                        .build());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException("createAcl failed: " + resp.getError().getMessage());
        }
    }

    /** Delete the ACL entry with this identity key (allow flag is not part of the key). */
    public void deleteAcl(
            String principal, String resourceType, String resourceName, boolean prefix, String operation) {
        var resp = admin.withDeadlineAfter(10, TimeUnit.SECONDS)
                .deleteAcl(jbroker.proto.broker.DeleteAclRequest.newBuilder()
                        .setEntry(aclEntry(principal, resourceType, resourceName, prefix, operation, true))
                        .build());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException("deleteAcl failed: " + resp.getError().getMessage());
        }
    }

    /** Every ACL entry in the responding broker's replicated cache. */
    public List<jbroker.proto.broker.AclEntry> listAcls() {
        var resp = admin.withDeadlineAfter(5, TimeUnit.SECONDS)
                .listAcls(jbroker.proto.broker.ListAclsRequest.getDefaultInstance());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException("listAcls failed: " + resp.getError().getMessage());
        }
        return resp.getEntriesList();
    }

    private static jbroker.proto.broker.AclEntry aclEntry(
            String principal,
            String resourceType,
            String resourceName,
            boolean prefix,
            String operation,
            boolean allow) {
        return jbroker.proto.broker.AclEntry.newBuilder()
                .setPrincipal(principal)
                .setResourceType(resourceType)
                .setResourceName(resourceName)
                .setPrefix(prefix)
                .setOperation(operation)
                .setAllow(allow)
                .build();
    }

    /** Create a topic with a config map. */
    public void createTopicWithConfig(String topic, int partitions, int rf, java.util.Map<String, String> config) {
        var b = CreateTopicRequest.newBuilder()
                .setTopic(topic)
                .setPartitions(partitions)
                .setReplicationFactor(rf);
        b.putAllConfig(config);
        var resp = admin.withDeadlineAfter(10, TimeUnit.SECONDS).createTopic(b.build());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException("createTopic failed: " + resp.getError().getMessage());
        }
    }

    /**
     * Allocate a fresh idempotent-producer id. Returns the assigned
     * {@code producer_id}; epoch is always 0 on initial allocation. Feed
     * the returned id to {@link #idempotentProduce} together with a
     * client-tracked {@code base_sequence} per {@code (topic, partition)}.
     */
    public long initProducerId() {
        var resp = producer.withDeadlineAfter(5, TimeUnit.SECONDS)
                .initProducerId(InitProducerIdRequest.newBuilder().build());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException(
                    "initProducerId failed: " + resp.getError().getMessage());
        }
        return resp.getProducerId();
    }

    // ---- Produce ----

    /** Produce a single record with leader-only acks (default). */
    public long produce(String topic, int partition, byte[] value) {
        return produceWithAcks(topic, partition, value, /*acks*/ 1);
    }

    /**
     * Send a batch of records in a single RPC — the Kafka-style batched
     * produce path. One RecordBatch wraps all values; offsets within the
     * batch are assigned contiguously starting at 0 (the broker remaps to
     * absolute offsets on append). Returns the absolute offset of the
     * last record.
     *
     * <p>This is the throughput lever: one-record-per-RPC is RTT-bound
     * (throughput ≤ 1/p50), a batched produce amortizes the network +
     * handler fixed cost across the whole batch.
     */
    public long produceBatch(String topic, int partition, List<byte[]> values) {
        return produceBatchWithAcks(topic, partition, values, /*acks*/ 1);
    }

    /** Batched produce with acks=all — blocks until every ISR member has the batch. */
    public long produceBatchAcksAll(String topic, int partition, List<byte[]> values) {
        return produceBatchWithAcks(topic, partition, values, /*acks*/ -1);
    }

    private long produceBatchWithAcks(String topic, int partition, List<byte[]> values, int acks) {
        if (values.isEmpty()) throw new IllegalArgumentException("values must be non-empty");
        var records = new java.util.ArrayList<Record>(values.size());
        for (int i = 0; i < values.size(); i++) {
            // offsetDelta within the batch is i; timestampDelta stays 0.
            records.add(new Record(i, 0L, null, values.get(i)));
        }
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        long now = System.currentTimeMillis();
        RecordBatch.encode(buf, 0L, 0, now, now, -1L, (short) -1, -1, records);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        long deadlineSeconds = acks == -1 ? 10 : 7;
        var resp = producer.withDeadlineAfter(deadlineSeconds, TimeUnit.SECONDS)
                .produce(ProduceRequest.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .setBatch(ByteString.copyFrom(bytes))
                        .setProducerId(-1L)
                        .setBaseSequence(-1)
                        .setAcks(acks)
                        .build());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException("produce failed: " + resp.getError().getMessage());
        }
        return resp.getLastOffset();
    }

    /**
     * Produce a single record and block until every ISR member has
     * replicated it (acks=all). Throws on timeout (the leader rejects
     * with {@code NOT_ENOUGH_REPLICAS}).
     */
    public long produceAcksAll(String topic, int partition, byte[] value) {
        return produceWithAcks(topic, partition, value, /*acks*/ -1);
    }

    private long produceWithAcks(String topic, int partition, byte[] value, int acks) {
        var records = List.of(new Record(0, 0L, null, value));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        long now = System.currentTimeMillis();
        RecordBatch.encode(buf, 0L, 0, now, now, -1L, (short) -1, -1, records);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        // acks=all may legitimately block for several seconds while
        // followers catch up; use a deadline a hair longer than the
        // broker's internal ACKS_ALL_TIMEOUT_MS (5s) so the server-side
        // NOT_ENOUGH_REPLICAS error surfaces to the caller rather than
        // a gRPC DEADLINE_EXCEEDED.
        long deadlineSeconds = acks == -1 ? 7 : 5;
        var resp = producer.withDeadlineAfter(deadlineSeconds, TimeUnit.SECONDS)
                .produce(ProduceRequest.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .setBatch(ByteString.copyFrom(bytes))
                        // Explicit legacy sentinel — proto3 int64 defaults to
                        // 0, which would otherwise trip the broker's
                        // idempotent-producer dedup.
                        .setProducerId(-1L)
                        .setBaseSequence(-1)
                        .setAcks(acks)
                        .build());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException("produce failed: " + resp.getError().getMessage());
        }
        return resp.getLastOffset();
    }

    /**
     * Idempotent variant: carries the producer id + epoch + base
     * sequence the caller obtained from {@link #initProducerId}. The broker
     * de-duplicates retries with the same sequence within a
     * {@code (topic, partition, producer_id, producer_epoch)} scope.
     */
    public long idempotentProduce(
            String topic, int partition, byte[] value, long producerId, int epoch, int baseSequence) {
        return idempotentProduceWithAcks(topic, partition, value, producerId, epoch, baseSequence, /*acks*/ 1);
    }

    /**
     * Idempotent produce with {@code acks=all} — blocks until every ISR
     * member has the record. Tests that need replication guaranteed before
     * a subsequent failover step (e.g., idempotent-dedup-across-failover)
     * use this variant; in-cluster producers typically do not.
     */
    public long idempotentProduceAcksAll(
            String topic, int partition, byte[] value, long producerId, int epoch, int baseSequence) {
        return idempotentProduceWithAcks(topic, partition, value, producerId, epoch, baseSequence, /*acks*/ -1);
    }

    /**
     * Idempotent batched produce with {@code acks=all} — the building
     * block for an async batching producer. One RecordBatch wraps all values
     * (offsets contiguous from 0, like {@link #produceBatch}) and carries the
     * caller's {@code (producer_id, epoch, base_sequence)}, so a retry of the
     * SAME base sequence dedupes to the cached offsets instead of
     * double-appending. Record {@code i} implicitly occupies sequence slot
     * {@code baseSequence + i}; the caller's next batch for this partition
     * must therefore start at {@code baseSequence + values.size()}. Returns
     * the absolute offset of the last record (the cached one on a deduped
     * retry).
     */
    public long idempotentProduceBatchAcksAll(
            String topic, int partition, List<byte[]> values, long producerId, int epoch, int baseSequence) {
        return idempotentProduceBatchAcksAll(
                topic, partition, values, producerId, epoch, baseSequence, jbroker.storage.Compression.NONE);
    }

    /**
     * Codec-carrying variant: the records section of the wire batch is
     * compressed client-side with {@code codec}, and the broker preserves
     * the codec when it re-encodes the batch for its log — compressed on
     * the wire, compressed on disk, decompressed transparently on fetch.
     */
    public long idempotentProduceBatchAcksAll(
            String topic,
            int partition,
            List<byte[]> values,
            long producerId,
            int epoch,
            int baseSequence,
            jbroker.storage.Compression codec) {
        if (values.isEmpty()) throw new IllegalArgumentException("values must be non-empty");
        var records = new java.util.ArrayList<Record>(values.size());
        for (int i = 0; i < values.size(); i++) {
            records.add(new Record(i, 0L, null, values.get(i)));
        }
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        long now = System.currentTimeMillis();
        RecordBatch.encode(buf, 0L, 0, now, now, producerId, (short) epoch, baseSequence, records, codec);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        var resp = producer.withDeadlineAfter(10, TimeUnit.SECONDS)
                .produce(ProduceRequest.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .setBatch(ByteString.copyFrom(bytes))
                        .setProducerId(producerId)
                        .setProducerEpoch(epoch)
                        .setBaseSequence(baseSequence)
                        .setAcks(-1)
                        .build());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException("produce failed: " + resp.getError().getMessage());
        }
        return resp.getLastOffset();
    }

    private long idempotentProduceWithAcks(
            String topic, int partition, byte[] value, long producerId, int epoch, int baseSequence, int acks) {
        var records = List.of(new Record(0, 0L, null, value));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        long now = System.currentTimeMillis();
        RecordBatch.encode(buf, 0L, 0, now, now, producerId, (short) epoch, baseSequence, records);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        long deadlineSeconds = acks == -1 ? 7 : 5;
        var resp = producer.withDeadlineAfter(deadlineSeconds, TimeUnit.SECONDS)
                .produce(ProduceRequest.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .setBatch(ByteString.copyFrom(bytes))
                        .setProducerId(producerId)
                        .setProducerEpoch(epoch)
                        .setBaseSequence(baseSequence)
                        .setAcks(acks)
                        .build());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException("produce failed: " + resp.getError().getMessage());
        }
        return resp.getLastOffset();
    }

    // ---- Consume ----

    /**
     * Fetch records starting at {@code offset}; returns the list of values in
     * offset order. {@code maxBytes} bounds the server-side response size.
     */
    public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) {
        var resp = consumer.withDeadlineAfter(5, TimeUnit.SECONDS)
                .fetch(FetchRequest.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .setOffset(offset)
                        .setMaxBytes(maxBytes)
                        .build());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException("fetch failed: " + resp.getError().getMessage());
        }
        var buf = ByteBuffer.wrap(resp.getRecords().toByteArray());
        var out = new java.util.ArrayList<byte[]>();
        while (buf.remaining() >= RecordBatch.BATCH_OVERHEAD) {
            int mark = buf.position();
            try {
                var parsed = RecordBatch.decode(buf);
                for (var rec : parsed.records()) {
                    if (parsed.baseOffset() + rec.offsetDelta() >= offset) {
                        out.add(rec.value());
                    }
                }
            } catch (IllegalArgumentException e) {
                buf.position(mark);
                break;
            }
        }
        return out;
    }

    /**
     * Offset-preserving fetch variant. Like {@link #fetch} but
     * surfaces each record's absolute offset and key alongside its value,
     * which integration tests (in particular the post-compaction sparse-
     * offset assertion) need to prove log-layer guarantees
     * survive the gRPC hop.
     */
    public List<FetchedRecord> fetchRecords(String topic, int partition, long offset, int maxBytes) {
        var resp = consumer.withDeadlineAfter(5, TimeUnit.SECONDS)
                .fetch(FetchRequest.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .setOffset(offset)
                        .setMaxBytes(maxBytes)
                        .build());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException("fetch failed: " + resp.getError().getMessage());
        }
        var buf = ByteBuffer.wrap(resp.getRecords().toByteArray());
        var out = new java.util.ArrayList<FetchedRecord>();
        while (buf.remaining() >= RecordBatch.BATCH_OVERHEAD) {
            int mark = buf.position();
            try {
                var parsed = RecordBatch.decode(buf);
                for (var rec : parsed.records()) {
                    long abs = parsed.baseOffset() + rec.offsetDelta();
                    if (abs >= offset) {
                        out.add(new FetchedRecord(abs, rec.key(), rec.value()));
                    }
                }
            } catch (IllegalArgumentException e) {
                buf.position(mark);
                break;
            }
        }
        return out;
    }

    /** Offset + key + value tuple from {@link #fetchRecords}. */
    public record FetchedRecord(long offset, byte[] key, byte[] value) {}

    /**
     * Fetch everything starting from offset 0 until no new records arrive.
     * Pages by the last record's actual offset — retention and sparse
     * compaction leave gaps, so counting records would re-request (and
     * re-receive) the same page forever once the log no longer starts at 0.
     */
    public List<byte[]> fetchAll(String topic, int partition, int maxBytes) {
        var all = new java.util.ArrayList<byte[]>();
        long offset = 0;
        while (true) {
            var batch = fetchRecords(topic, partition, offset, maxBytes);
            if (batch.isEmpty()) break;
            for (var rec : batch) {
                all.add(rec.value());
            }
            offset = batch.get(batch.size() - 1).offset() + 1;
        }
        return all;
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @SuppressWarnings("unused")
    private static ByteArrayOutputStream scratch() {
        return new ByteArrayOutputStream();
    }
}
