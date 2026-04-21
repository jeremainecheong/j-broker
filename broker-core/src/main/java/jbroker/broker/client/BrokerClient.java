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
import jbroker.proto.broker.DescribeTopicRequest;
import jbroker.proto.broker.FetchRequest;
import jbroker.proto.broker.ListTopicsRequest;
import jbroker.proto.broker.ProduceRequest;
import jbroker.proto.broker.ProducerGrpc;
import jbroker.proto.broker.TopicDescription;
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

    // ---- Produce ----

    /** Produce a single record to a specific partition; returns the assigned offset. */
    public long produce(String topic, int partition, byte[] value) {
        var records = List.of(new Record(0, 0L, null, value));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        long now = System.currentTimeMillis();
        RecordBatch.encode(buf, 0L, 0, now, now, -1L, (short) -1, -1, records);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        var resp = producer.withDeadlineAfter(5, TimeUnit.SECONDS)
                .produce(ProduceRequest.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .setBatch(ByteString.copyFrom(bytes))
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
