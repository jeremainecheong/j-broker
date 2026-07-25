package jbroker.storage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

/**
 * Contract of the control and transactional attribute bits: control batches
 * round-trip their marker exactly and are distinguishable without touching
 * record payloads; transactional data batches carry their flag through
 * every codec; malformed combinations (compressed control, multi-record
 * control, flags without producer identity) are refused loudly.
 */
class RecordBatchControlTest {

    @Test
    void controlBatchRoundTripsMarkerAndIdentity() {
        var marker = new ControlRecord(ControlRecord.Type.COMMIT, 11);
        var buf = ByteBuffer.allocate(256);
        int written = RecordBatch.encodeControl(buf, 40L, 3, 5_000L, 9L, (short) 2, marker);

        buf.flip();
        var parsed = RecordBatch.decode(buf);
        assertThat(parsed.control()).isTrue();
        assertThat(parsed.transactional()).isFalse();
        assertThat(parsed.codec()).isEqualTo(Compression.NONE);
        assertThat(parsed.baseOffset()).isEqualTo(40L);
        assertThat(parsed.lastOffset())
                .as("a control batch consumes one offset")
                .isEqualTo(40L);
        assertThat(parsed.partitionLeaderEpoch()).isEqualTo(3);
        assertThat(parsed.producerId()).isEqualTo(9L);
        assertThat(parsed.producerEpoch()).isEqualTo((short) 2);
        assertThat(parsed.baseSequence()).isEqualTo(-1);
        assertThat(parsed.records()).hasSize(1);
        assertThat(parsed.controlRecord()).isEqualTo(marker);
        assertThat(buf.position()).as("decode consumed exactly one batch").isEqualTo(written);
    }

    @Test
    void abortMarkerRoundTrips() {
        var marker = new ControlRecord(ControlRecord.Type.ABORT, 0);
        var buf = ByteBuffer.allocate(256);
        RecordBatch.encodeControl(buf, 0L, 0, 0L, 1L, (short) 0, marker);
        buf.flip();
        assertThat(RecordBatch.decode(buf).controlRecord()).isEqualTo(marker);
    }

    @Test
    void controlBitReadableWithoutDecode() {
        var buf = ByteBuffer.allocate(256);
        RecordBatch.encodeControl(buf, 0L, 0, 0L, 5L, (short) 1, new ControlRecord(ControlRecord.Type.COMMIT, 1));
        short attributes = buf.getShort(RecordBatch.ATTRIBUTES_OFFSET);
        assertThat(RecordBatch.isControl(attributes)).isTrue();
        assertThat(RecordBatch.isTransactional(attributes)).isFalse();
        assertThat(buf.getLong(RecordBatch.PRODUCER_ID_OFFSET)).isEqualTo(5L);
    }

    @Test
    void transactionalDataBatchRoundTripsPlain() {
        var records = List.of(new Record(0, 0L, "k".getBytes(UTF_8), "v".getBytes(UTF_8)));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        RecordBatch.encode(buf, 10L, 1, 100L, 100L, 7L, (short) 0, 0, records, Compression.NONE, true);

        assertThat(RecordBatch.isTransactional(buf.getShort(RecordBatch.ATTRIBUTES_OFFSET)))
                .isTrue();
        buf.flip();
        var parsed = RecordBatch.decode(buf);
        assertThat(parsed.transactional()).isTrue();
        assertThat(parsed.control()).isFalse();
        assertThat(parsed.producerId()).isEqualTo(7L);
        assertThat(parsed.records().get(0).value()).containsExactly("v".getBytes(UTF_8));
    }

    @Test
    void transactionalBitSurvivesCompression() {
        var records = List.of(new Record(0, 0L, null, "payload-".repeat(50).getBytes(UTF_8)));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        RecordBatch.encode(buf, 0L, 0, 0L, 0L, 3L, (short) 1, 5, records, Compression.ZSTD, true);
        buf.flip();
        var parsed = RecordBatch.decode(buf);
        assertThat(parsed.codec()).isEqualTo(Compression.ZSTD);
        assertThat(parsed.transactional()).isTrue();
        assertThat(parsed.records().get(0).value())
                .containsExactly("payload-".repeat(50).getBytes(UTF_8));
    }

    @Test
    void plainDataBatchReportsNeitherFlag() {
        var records = List.of(new Record(0, 0L, null, new byte[] {1}));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        RecordBatch.encode(buf, 0L, 0, 0L, 0L, -1L, (short) -1, -1, records);
        buf.flip();
        var parsed = RecordBatch.decode(buf);
        assertThat(parsed.control()).isFalse();
        assertThat(parsed.transactional()).isFalse();
    }

    @Test
    void transactionalWithoutProducerIdRefuses() {
        var records = List.of(new Record(0, 0L, null, new byte[] {1}));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        assertThatThrownBy(() ->
                        RecordBatch.encode(buf, 0L, 0, 0L, 0L, -1L, (short) -1, -1, records, Compression.NONE, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("producerId");
    }

    @Test
    void controlWithoutProducerIdRefuses() {
        var buf = ByteBuffer.allocate(256);
        assertThatThrownBy(() -> RecordBatch.encodeControl(
                        buf, 0L, 0, 0L, -1L, (short) 0, new ControlRecord(ControlRecord.Type.COMMIT, 0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("producerId");
    }

    @Test
    void compressedControlBatchRefuses() {
        // No writer produces this shape; forge it the way a corrupting
        // intermediary would — flip the control bit onto a zstd batch and
        // restamp the CRC so decode reaches the invariant check.
        var records = List.of(new Record(0, 0L, null, "compressible-".repeat(40).getBytes(UTF_8)));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        int written = RecordBatch.encode(buf, 0L, 0, 0L, 0L, 1L, (short) 0, 0, records, Compression.ZSTD);
        stampAttributes(buf, written, (short) (buf.getShort(RecordBatch.ATTRIBUTES_OFFSET) | RecordBatch.ATTR_CONTROL));
        buf.flip();
        assertThatThrownBy(() -> RecordBatch.decode(buf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("control batch must not be compressed");
    }

    @Test
    void multiRecordControlBatchRefuses() {
        var records = List.of(new Record(0, 0L, null, new byte[] {1}), new Record(1, 0L, null, new byte[] {2}));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        int written = RecordBatch.encode(buf, 0L, 0, 0L, 0L, 1L, (short) 0, 0, records);
        stampAttributes(buf, written, RecordBatch.ATTR_CONTROL);
        buf.flip();
        assertThatThrownBy(() -> RecordBatch.decode(buf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one record");
    }

    @Test
    void corruptControlBatchFailsCrc() {
        var buf = ByteBuffer.allocate(256);
        int written = RecordBatch.encodeControl(
                buf, 0L, 0, 0L, 1L, (short) 0, new ControlRecord(ControlRecord.Type.ABORT, 4));
        buf.put(written - 1, (byte) (buf.get(written - 1) ^ 0x01));
        buf.flip();
        assertThatThrownBy(() -> RecordBatch.decode(buf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CRC");
    }

    @Test
    void controlRecordAccessorRefusesOnDataBatch() {
        var records = List.of(new Record(0, 0L, null, new byte[] {1}));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        RecordBatch.encode(buf, 0L, 0, 0L, 0L, -1L, (short) -1, -1, records);
        buf.flip();
        var parsed = RecordBatch.decode(buf);
        assertThatThrownBy(parsed::controlRecord)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not a control batch");
    }

    /** Overwrite the attributes of an encoded batch and restamp the CRC. */
    private static void stampAttributes(ByteBuffer buf, int written, short attributes) {
        buf.putShort(RecordBatch.ATTRIBUTES_OFFSET, attributes);
        var crc = new CRC32C();
        var view = buf.duplicate();
        view.position(RecordBatch.ATTRIBUTES_OFFSET);
        view.limit(written);
        crc.update(view);
        buf.putInt(17, (int) crc.getValue());
    }
}
