package jbroker.broker.txn;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;
import jbroker.proto.common.TopicPartition;

/**
 * Constants + binary key/value codec for the {@code __transaction_state}
 * internal topic, following the {@link jbroker.broker.ConsumerOffsetsTopic}
 * conventions: a leading record-type byte on the key, {@code writeUTF}
 * 16-bit-length-prefixed strings, big-endian integers, and decoders that
 * skip unknown record-type bytes (forward compat).
 *
 * <p>One record type lives in the topic today:
 * <ul>
 *   <li><b>Type 1 — transaction state.</b><br>
 *       Key bytes: {@code 0x01 | utf8(transactional_id)}.<br>
 *       Value bytes: {@code int64(producer_id) | int32(producer_epoch) |
 *       byte(state) | int32(timeout_ms) | int64(last_update_ms) |
 *       int32(partition_count) | (utf8(topic) int32(partition))*}.</li>
 * </ul>
 */
public final class TxnStateTopic {

    /** Topic name. Compaction marker is propagated through {@code TopicRecord.compact}. */
    public static final String NAME = "__transaction_state";

    /** Fixed partition count — changing this invalidates every transactional_id's coordinator routing. */
    public static final int PARTITION_COUNT = 50;

    private static final byte TYPE_TXN_STATE = 0x01;

    private TxnStateTopic() {}

    /**
     * Coordinator routing: the partition of {@code NAME} whose leader
     * coordinates {@code transactionalId}. Same convention as consumer
     * groups over {@code __consumer_offsets}.
     */
    public static int partitionFor(String transactionalId, int partitionCount) {
        return Math.floorMod(transactionalId.hashCode(), partitionCount);
    }

    public static byte[] keyForTxn(String transactionalId) {
        try (var baos = new ByteArrayOutputStream();
                var dout = new DataOutputStream(baos)) {
            dout.writeByte(TYPE_TXN_STATE);
            dout.writeUTF(transactionalId);
            return baos.toByteArray();
        } catch (IOException e) {
            // ByteArrayOutputStream never throws.
            throw new AssertionError(e);
        }
    }

    public static Optional<String> decodeTxnKey(byte[] bytes) {
        if (bytes == null || bytes.length < 1 || bytes[0] != TYPE_TXN_STATE) {
            return Optional.empty();
        }
        try (var din = new DataInputStream(new ByteArrayInputStream(bytes, 1, bytes.length - 1))) {
            return Optional.of(din.readUTF());
        } catch (IOException e) {
            // Truncated / corrupt key. Forward-compat decoders skip such records.
            return Optional.empty();
        }
    }

    public static byte[] valueForTxnState(TxnStateRecord rec) {
        try (var baos = new ByteArrayOutputStream();
                var dout = new DataOutputStream(baos)) {
            dout.writeLong(rec.producerId());
            dout.writeInt(rec.producerEpoch());
            dout.writeByte(rec.state().wireCode());
            dout.writeInt(rec.timeoutMs());
            dout.writeLong(rec.lastUpdateMs());
            dout.writeInt(rec.partitions().size());
            for (var tp : rec.partitions()) {
                dout.writeUTF(tp.getTopic());
                dout.writeInt(tp.getPartition());
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /** Inverse of {@link #valueForTxnState}; {@code transactionalId} comes from the key. */
    public static TxnStateRecord decodeTxnValue(String transactionalId, byte[] bytes) {
        try (var din = new DataInputStream(new ByteArrayInputStream(bytes))) {
            long producerId = din.readLong();
            int producerEpoch = din.readInt();
            var state = TxnState.fromWireCode(din.readByte());
            int timeoutMs = din.readInt();
            long lastUpdateMs = din.readLong();
            int partitionCount = din.readInt();
            var partitions = new ArrayList<TopicPartition>(partitionCount);
            for (int i = 0; i < partitionCount; i++) {
                String topic = din.readUTF();
                int partition = din.readInt();
                partitions.add(TopicPartition.newBuilder()
                        .setTopic(topic)
                        .setPartition(partition)
                        .build());
            }
            return new TxnStateRecord(
                    transactionalId, producerId, producerEpoch, state, partitions, timeoutMs, lastUpdateMs);
        } catch (IOException e) {
            throw new IllegalArgumentException("malformed txn state value: " + e.getMessage(), e);
        }
    }
}
