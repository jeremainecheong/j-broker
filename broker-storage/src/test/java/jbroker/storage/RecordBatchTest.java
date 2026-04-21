package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

class RecordBatchTest {

    @Test
    void roundTripsSingleRecord() {
        var records = List.of(new Record(0, 0L, "key".getBytes(), "hello".getBytes()));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        int written = RecordBatch.encode(buf, 100L, 7, 1_700_000_000L, 1_700_000_000L, -1L, (short) -1, -1, records);
        assertThat(written).isEqualTo(buf.position());

        buf.flip();
        var parsed = RecordBatch.decode(buf);
        assertThat(parsed.baseOffset()).isEqualTo(100L);
        assertThat(parsed.partitionLeaderEpoch()).isEqualTo(7);
        assertThat(parsed.records()).hasSize(1);
        assertThat(parsed.records().get(0).value()).containsExactly("hello".getBytes());
        assertThat(parsed.records().get(0).key()).containsExactly("key".getBytes());
        assertThat(parsed.lastOffset()).isEqualTo(100L);
    }

    @Test
    void roundTripsMultipleRecordsPreservingOffsetDeltas() {
        var records = List.of(
                new Record(0, 0L, null, new byte[] {1}),
                new Record(1, 5L, null, new byte[] {2}),
                new Record(2, 12L, null, new byte[] {3}));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        RecordBatch.encode(buf, 500L, 0, 1_000L, 1_012L, -1L, (short) -1, -1, records);
        buf.flip();
        var parsed = RecordBatch.decode(buf);

        assertThat(parsed.records()).hasSize(3);
        assertThat(parsed.lastOffset()).isEqualTo(502L);
        assertThat(parsed.records().get(2).value()).containsExactly(3);
    }

    @Test
    void detectsCorruptCrc() {
        var records = List.of(new Record(0, 0L, null, new byte[] {1, 2, 3}));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        RecordBatch.encode(buf, 0L, 0, 0L, 0L, -1L, (short) -1, -1, records);
        // Flip a bit inside the value payload.
        int lastByte = buf.position() - 1;
        buf.put(lastByte, (byte) (buf.get(lastByte) ^ 0x01));

        buf.flip();
        assertThatThrownBy(() -> RecordBatch.decode(buf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CRC");
    }

    @Test
    void rejectsWrongMagic() {
        var records = List.of(new Record(0, 0L, null, new byte[] {9}));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        RecordBatch.encode(buf, 0L, 0, 0L, 0L, -1L, (short) -1, -1, records);
        // Overwrite magic byte at offset 16.
        buf.put(16, (byte) 1);
        buf.flip();
        assertThatThrownBy(() -> RecordBatch.decode(buf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("magic");
    }
}
