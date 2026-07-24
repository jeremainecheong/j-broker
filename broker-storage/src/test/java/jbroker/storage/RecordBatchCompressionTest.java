package jbroker.storage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

/**
 * Contract of the attributes codec bits: zstd batches round-trip exactly,
 * the header stays readable without decompression, reserved codec ids are
 * rejected, and {@link RecordBatch#estimatedSize} bounds the stored size
 * for every codec (compression is dropped when it would not shrink the
 * records section).
 */
class RecordBatchCompressionTest {

    @Test
    void zstdRoundTripRecoversRecordsExactly() {
        var records = compressibleRecords(50);
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        int written = RecordBatch.encode(buf, 100L, 7, 1_000L, 1_050L, 42L, (short) 3, 17, records, Compression.ZSTD);

        assertThat(written).isLessThan(RecordBatch.estimatedSize(records));

        buf.flip();
        var parsed = RecordBatch.decode(buf);
        assertThat(parsed.codec()).isEqualTo(Compression.ZSTD);
        assertThat(parsed.baseOffset()).isEqualTo(100L);
        assertThat(parsed.partitionLeaderEpoch()).isEqualTo(7);
        assertThat(parsed.producerId()).isEqualTo(42L);
        assertThat(parsed.producerEpoch()).isEqualTo((short) 3);
        assertThat(parsed.baseSequence()).isEqualTo(17);
        assertThat(parsed.records()).hasSize(records.size());
        for (int i = 0; i < records.size(); i++) {
            assertThat(parsed.records().get(i).offsetDelta())
                    .isEqualTo(records.get(i).offsetDelta());
            assertThat(parsed.records().get(i).key())
                    .containsExactly(records.get(i).key());
            assertThat(parsed.records().get(i).value())
                    .containsExactly(records.get(i).value());
        }
        assertThat(buf.position()).as("decode consumed exactly one batch").isEqualTo(written);
    }

    @Test
    void headerFieldsReadableWithoutDecompression() {
        var records = compressibleRecords(20);
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        RecordBatch.encode(buf, 512L, 9, 2_000L, 2_020L, 77L, (short) 1, 40, records, Compression.ZSTD);

        // Fixed header offsets straight off the stored bytes — no decode,
        // no decompression. This is the property replication and the
        // sparse index depend on.
        assertThat(buf.getLong(0)).isEqualTo(512L); // baseOffset
        assertThat(buf.get(16)).isEqualTo(RecordBatch.MAGIC_V2);
        assertThat(buf.getShort(21) & Compression.CODEC_MASK).isEqualTo(Compression.ZSTD.id());
        assertThat(buf.getInt(23)).isEqualTo(19); // lastOffsetDelta
        assertThat(buf.getLong(27)).isEqualTo(2_000L); // firstTimestamp
        assertThat(buf.getLong(35)).isEqualTo(2_020L); // maxTimestamp
        assertThat(buf.getLong(43)).isEqualTo(77L); // producerId
        assertThat(buf.getShort(51)).isEqualTo((short) 1); // producerEpoch
        assertThat(buf.getInt(53)).isEqualTo(40); // baseSequence
        assertThat(buf.getInt(57)).isEqualTo(20); // numRecords
    }

    @Test
    void reservedCodecIdsAreRejected() {
        for (int codecId : new int[] {1, 3, 4, 7}) {
            var records = List.of(new Record(0, 0L, null, "payload".getBytes(UTF_8)));
            var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
            int written = RecordBatch.encode(buf, 0L, 0, 0L, 0L, -1L, (short) -1, -1, records);
            stampCodecBits(buf, written, codecId);
            buf.flip();
            assertThatThrownBy(() -> RecordBatch.decode(buf))
                    .as("codec id %d", codecId)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("unsupported compression codec id " + codecId);
        }
    }

    @Test
    void corruptCompressedPayloadFailsCrcBeforeDecompression() {
        var records = compressibleRecords(20);
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        int written = RecordBatch.encode(buf, 0L, 0, 0L, 0L, -1L, (short) -1, -1, records, Compression.ZSTD);
        // Flip a bit inside the compressed records section.
        buf.put(written - 1, (byte) (buf.get(written - 1) ^ 0x01));
        buf.flip();
        assertThatThrownBy(() -> RecordBatch.decode(buf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CRC");
    }

    @Test
    void incompressiblePayloadStoresPlainAndStillDecodes() {
        byte[] noise = new byte[256];
        new Random(42).nextBytes(noise);
        var records = List.of(new Record(0, 0L, null, noise));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        int written = RecordBatch.encode(buf, 5L, 0, 0L, 0L, -1L, (short) -1, -1, records, Compression.ZSTD);

        // zstd on random bytes doesn't pay; the encoder must fall back so
        // estimatedSize stays an upper bound.
        assertThat(written).isLessThanOrEqualTo(RecordBatch.estimatedSize(records));

        buf.flip();
        var parsed = RecordBatch.decode(buf);
        assertThat(parsed.codec()).isEqualTo(Compression.NONE);
        assertThat(parsed.records().get(0).value()).containsExactly(noise);
    }

    @Test
    void singleTinyRecordRoundTripsUnderZstdRequest() {
        var records = List.of(new Record(0, 0L, null, new byte[] {1}));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        int written = RecordBatch.encode(buf, 0L, 0, 0L, 0L, -1L, (short) -1, -1, records, Compression.ZSTD);
        assertThat(written).isLessThanOrEqualTo(RecordBatch.estimatedSize(records));
        buf.flip();
        var parsed = RecordBatch.decode(buf);
        assertThat(parsed.records()).hasSize(1);
        assertThat(parsed.records().get(0).value()).containsExactly(new byte[] {1});
    }

    @Test
    void largeValueCompressesAndRoundTrips() {
        byte[] large = "the same phrase over and over ".repeat(35_000).getBytes(UTF_8); // ~1 MiB
        var records = List.of(new Record(0, 0L, "big".getBytes(UTF_8), large));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        int written = RecordBatch.encode(buf, 0L, 0, 0L, 0L, -1L, (short) -1, -1, records, Compression.ZSTD);

        assertThat(written).isLessThan(large.length / 2);

        buf.flip();
        var parsed = RecordBatch.decode(buf);
        assertThat(parsed.codec()).isEqualTo(Compression.ZSTD);
        assertThat(parsed.records().get(0).value()).containsExactly(large);
    }

    @Test
    void nullKeysTombstonesAndHeadersSurviveCompression() {
        byte[][] headers = {"h-key".getBytes(UTF_8), "h-value".getBytes(UTF_8)};
        var records = List.of(
                new Record(0, 0L, null, "no-key".repeat(30).getBytes(UTF_8)),
                new Record(1, 2L, "k1".getBytes(UTF_8), null), // tombstone
                new Record(2, 4L, "k2".getBytes(UTF_8), "with-header".repeat(30).getBytes(UTF_8), headers));
        var buf = ByteBuffer.allocate(RecordBatch.estimatedSize(records));
        RecordBatch.encode(buf, 0L, 0, 0L, 4L, -1L, (short) -1, -1, records, Compression.ZSTD);
        buf.flip();
        var parsed = RecordBatch.decode(buf);

        assertThat(parsed.codec()).isEqualTo(Compression.ZSTD);
        assertThat(parsed.records().get(0).key()).isNull();
        assertThat(parsed.records().get(1).value()).isNull();
        assertThat(parsed.records().get(1).key()).containsExactly("k1".getBytes(UTF_8));
        assertThat(parsed.records().get(2).headers()[0]).containsExactly("h-key".getBytes(UTF_8));
        assertThat(parsed.records().get(2).headers()[1]).containsExactly("h-value".getBytes(UTF_8));
    }

    @Test
    void emptyRecordsRejectedForEveryCodec() {
        var buf = ByteBuffer.allocate(RecordBatch.BATCH_OVERHEAD);
        assertThatThrownBy(
                        () -> RecordBatch.encode(buf, 0L, 0, 0L, 0L, -1L, (short) -1, -1, List.of(), Compression.ZSTD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-empty");
    }

    /** Repetitive keyed payloads — realistic and reliably compressible. */
    private static List<Record> compressibleRecords(int n) {
        var records = new ArrayList<Record>(n);
        for (int i = 0; i < n; i++) {
            byte[] key = ("key-" + (i % 5)).getBytes(UTF_8);
            byte[] value = ("payload-" + i + "-" + "abcdefgh".repeat(16)).getBytes(UTF_8);
            records.add(new Record(i, i, key, value));
        }
        return records;
    }

    /**
     * Overwrite the codec bits of an encoded batch and restamp the CRC so
     * decode reaches the codec check instead of failing the CRC — the shape
     * of a batch written by a binary with codecs this one lacks.
     */
    private static void stampCodecBits(ByteBuffer buf, int written, int codecId) {
        short attributes = buf.getShort(21);
        buf.putShort(21, (short) ((attributes & ~Compression.CODEC_MASK) | codecId));
        var crc = new CRC32C();
        var view = buf.duplicate();
        view.position(21);
        view.limit(written);
        crc.update(view);
        buf.putInt(17, (int) crc.getValue());
    }
}
