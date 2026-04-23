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

/**
 * Minimal single-broker client surface for Phase 5. Bundles Producer,
 * Consumer, and Admin stubs against one gRPC channel. No retries, no
 * batching — the simplest possible shape that lets the E2E test cover
 * "produce N, consume N" end-to-end.
 */
public final class BrokerClient implements AutoCloseable {

    private final ManagedChannel channel;
    private final ProducerGrpc.ProducerBlockingStub producer;
    private final ConsumerGrpc.ConsumerBlockingStub consumer;
    private final AdminGrpc.AdminBlockingStub admin;

    public BrokerClient(String host, int port) {
        this.channel = NettyChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.producer = ProducerGrpc.newBlockingStub(channel);
        this.consumer = ConsumerGrpc.newBlockingStub(channel);
        this.admin = AdminGrpc.newBlockingStub(channel);
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
        return admin.withDeadlineAfter(5, TimeUnit.SECONDS)
                .listTopics(ListTopicsRequest.newBuilder().build())
                .getTopicsList();
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

    /** Phase 8 — delete a topic cluster-wide. Raises on NOT_LEADER / IO / UNKNOWN_TOPIC. */
    public void deleteTopic(String topic) {
        var resp = admin.withDeadlineAfter(5, TimeUnit.SECONDS)
                .deleteTopic(DeleteTopicRequest.newBuilder().setTopic(topic).build());
        if (resp.hasError() && resp.getError().getCode() != 0) {
            throw new RuntimeException("deleteTopic failed: " + resp.getError().getMessage());
        }
    }

    /** Phase 8 — merge a config overlay into an existing topic. Returns the merged map. */
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
     * P13.1 — force synchronous compaction on the responding broker's local
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

    /** Phase 8 — create a topic with a config map. */
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
     * Allocate a fresh idempotent-producer id (P6.7). Returns the assigned
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
     * Idempotent variant (P6.7): carries the producer id + epoch + base
     * sequence the caller obtained from {@link #initProducerId}. The broker
     * de-duplicates retries with the same sequence within a
     * {@code (topic, partition, producer_id, producer_epoch)} scope.
     */
    public long idempotentProduce(
            String topic, int partition, byte[] value, long producerId, int epoch, int baseSequence) {
        var records = List.of(new Record(0, 0L, null, value));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        long now = System.currentTimeMillis();
        RecordBatch.encode(buf, 0L, 0, now, now, producerId, (short) epoch, baseSequence, records);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        var resp = producer.withDeadlineAfter(5, TimeUnit.SECONDS)
                .produce(ProduceRequest.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .setBatch(ByteString.copyFrom(bytes))
                        .setProducerId(producerId)
                        .setProducerEpoch(epoch)
                        .setBaseSequence(baseSequence)
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
     * P13.1 — offset-preserving fetch variant. Like {@link #fetch} but
     * surfaces each record's absolute offset and key alongside its value,
     * which integration tests (in particular the post-compaction sparse-
     * offset assertion in E2E-13-1) need to prove log-layer guarantees
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

    /** Fetch everything starting from offset 0 until no new records arrive. */
    public List<byte[]> fetchAll(String topic, int partition, int maxBytes) {
        var all = new java.util.ArrayList<byte[]>();
        long offset = 0;
        while (true) {
            var batch = fetch(topic, partition, offset, maxBytes);
            if (batch.isEmpty()) break;
            all.addAll(batch);
            offset += batch.size();
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
