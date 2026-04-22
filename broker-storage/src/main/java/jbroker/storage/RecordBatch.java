package jbroker.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * Kafka v2 record batch, simplified: uncompressed, no transactional fields.
 * Supports encode (for append) and decode (for fetch / recovery). Per-record
 * headers are optional — encoded as a varint count followed by
 * {@code (keyLen varint, key bytes, valueLen varint, value bytes)} pairs.
 *
 * <p>Wire format (big-endian):
 *
 * <pre>
 *  [0..7]   baseOffset                      int64
 *  [8..11]  batchLength (bytes AFTER this)  int32
 *  [12..15] partitionLeaderEpoch            int32
 *  [16]     magic (= 2)                     int8
 *  [17..20] crc (CRC-32C of bytes [21..])   uint32
 *  [21..22] attributes                      int16
 *  [23..26] lastOffsetDelta                 int32
 *  [27..34] firstTimestamp                  int64
 *  [35..42] maxTimestamp                    int64
 *  [43..50] producerId                      int64
 *  [51..52] producerEpoch                   int16
 *  [53..56] baseSequence                    int32
 *  [57..60] numRecords                      int32
 *  [61..]   records
 * </pre>
 *
 * Record layout (Kafka-v2):
 *
 * <pre>
 *  length           varint (size of rest of this record)
 *  attributes       int8
 *  timestampDelta   varlong
 *  offsetDelta      varint
 *  keyLength        varint (-1 ⇒ null key)
 *  key              byte[keyLength] (omitted if null)
 *  valueLength      varint (-1 ⇒ null value)
 *  value            byte[valueLength]
 *  numHeaders       varint
 *  headers          numHeaders × (headerKeyLen varint, headerKey bytes,
 *                                 headerValueLen varint, headerValue bytes)
 * </pre>
 */
public final class RecordBatch {

    public static final byte MAGIC_V2 = 2;
    public static final int BATCH_OVERHEAD = 61; // bytes before records[]
    private static final int CRC_OFFSET = 17;
    private static final int ATTRIBUTES_OFFSET = 21;

    private RecordBatch() {}

    /** Parsed batch returned by {@link #decode}. */
    public record Parsed(
            long baseOffset,
            int batchLength,
            int partitionLeaderEpoch,
            int lastOffsetDelta,
            long firstTimestamp,
            long maxTimestamp,
            long producerId,
            short producerEpoch,
            int baseSequence,
            List<Record> records) {

        public long lastOffset() {
            return baseOffset + lastOffsetDelta;
        }

        public int totalBytes() {
            return 12 + batchLength; // baseOffset(8) + batchLength(4) + rest
        }
    }

    /**
     * Encode a batch. {@code out} must be big-endian and have at least
     * {@link #estimatedSize} bytes of remaining capacity.
     */
    public static int encode(
            ByteBuffer out,
            long baseOffset,
            int partitionLeaderEpoch,
            long firstTimestamp,
            long maxTimestamp,
            long producerId,
            short producerEpoch,
            int baseSequence,
            List<Record> records) {
        if (records.isEmpty()) throw new IllegalArgumentException("records must be non-empty");
        out.order(ByteOrder.BIG_ENDIAN);
        int startPos = out.position();

        out.putLong(baseOffset);
        int batchLengthPos = out.position();
        out.putInt(0); // placeholder
        out.putInt(partitionLeaderEpoch);
        out.put(MAGIC_V2);
        int crcPos = out.position();
        out.putInt(0); // crc placeholder
        int crcStart = out.position();
        out.putShort((short) 0); // attributes
        int lastOffsetDelta = records.get(records.size() - 1).offsetDelta();
        out.putInt(lastOffsetDelta);
        out.putLong(firstTimestamp);
        out.putLong(maxTimestamp);
        out.putLong(producerId);
        out.putShort(producerEpoch);
        out.putInt(baseSequence);
        out.putInt(records.size());
        for (var r : records) {
            encodeRecord(out, r);
        }
        int endPos = out.position();

        // Fill in batchLength (= bytes after the batchLength field itself, so
        // from partitionLeaderEpoch through end-of-records).
        int batchLength = endPos - batchLengthPos - 4;
        out.putInt(batchLengthPos, batchLength);

        // Compute CRC-32C over [crcStart..endPos).
        var crc = new CRC32C();
        out.position(crcStart);
        out.limit(endPos);
        crc.update(out);
        out.limit(out.capacity());
        int crcValue = (int) crc.getValue();
        out.putInt(crcPos, crcValue);

        out.position(endPos);
        return endPos - startPos;
    }

    private static void encodeRecord(ByteBuffer out, Record r) {
        int inner = sizeOfRecordInner(r);
        Varints.writeVarint(inner, out);
        out.put((byte) 0); // attributes
        Varints.writeVarlong(r.timestampDelta(), out);
        Varints.writeVarint(r.offsetDelta(), out);
        if (r.key() == null) {
            Varints.writeVarint(-1, out);
        } else {
            Varints.writeVarint(r.key().length, out);
            out.put(r.key());
        }
        if (r.value() == null) {
            Varints.writeVarint(-1, out);
        } else {
            Varints.writeVarint(r.value().length, out);
            out.put(r.value());
        }
        var hdr = r.headers();
        int pairCount = hdr.length / 2;
        Varints.writeVarint(pairCount, out);
        for (int i = 0; i < pairCount; i++) {
            byte[] hk = hdr[i * 2];
            byte[] hv = hdr[i * 2 + 1];
            if (hk == null) {
                Varints.writeVarint(-1, out);
            } else {
                Varints.writeVarint(hk.length, out);
                out.put(hk);
            }
            if (hv == null) {
                Varints.writeVarint(-1, out);
            } else {
                Varints.writeVarint(hv.length, out);
                out.put(hv);
            }
        }
    }

    /** Size of the inner-record bytes (everything past the length-prefix varint). */
    private static int sizeOfRecordInner(Record r) {
        int size = 1; // attributes
        size += Varints.sizeOfVarlong(r.timestampDelta());
        size += Varints.sizeOfVarint(r.offsetDelta());
        if (r.key() == null) {
            size += Varints.sizeOfVarint(-1);
        } else {
            size += Varints.sizeOfVarint(r.key().length) + r.key().length;
        }
        if (r.value() == null) {
            size += Varints.sizeOfVarint(-1);
        } else {
            size += Varints.sizeOfVarint(r.value().length) + r.value().length;
        }
        var hdr = r.headers();
        int pairCount = hdr.length / 2;
        size += Varints.sizeOfVarint(pairCount);
        for (int i = 0; i < pairCount; i++) {
            byte[] hk = hdr[i * 2];
            byte[] hv = hdr[i * 2 + 1];
            if (hk == null) {
                size += Varints.sizeOfVarint(-1);
            } else {
                size += Varints.sizeOfVarint(hk.length) + hk.length;
            }
            if (hv == null) {
                size += Varints.sizeOfVarint(-1);
            } else {
                size += Varints.sizeOfVarint(hv.length) + hv.length;
            }
        }
        return size;
    }

    /** Upper bound on encoded size; useful for buffer sizing. */
    public static int estimatedSize(List<Record> records) {
        int size = BATCH_OVERHEAD;
        for (var r : records) {
            int inner = sizeOfRecordInner(r);
            size += Varints.sizeOfVarint(inner) + inner;
        }
        return size;
    }

    /**
     * Decode a batch from {@code in}, starting at its current position. On
     * return, the buffer's position is advanced to the byte after this batch.
     *
     * @throws IllegalArgumentException if CRC fails, magic is wrong, or the
     *         buffer runs short.
     */
    public static Parsed decode(ByteBuffer in) {
        in.order(ByteOrder.BIG_ENDIAN);
        if (in.remaining() < BATCH_OVERHEAD) {
            throw new IllegalArgumentException("short batch: " + in.remaining() + " < " + BATCH_OVERHEAD);
        }
        int start = in.position();
        long baseOffset = in.getLong();
        int batchLength = in.getInt();
        int totalRemaining = batchLength; // bytes after the batchLength field
        if (in.remaining() < totalRemaining) {
            throw new IllegalArgumentException(
                    "batch truncated: declared " + totalRemaining + " have " + in.remaining());
        }
        int batchEnd = in.position() + totalRemaining;

        int partitionLeaderEpoch = in.getInt();
        byte magic = in.get();
        if (magic != MAGIC_V2) throw new IllegalArgumentException("unsupported magic: " + magic);
        int crcRead = in.getInt();

        int crcStart = in.position();
        // CRC covers [crcStart..batchEnd)
        var crc = new CRC32C();
        int savedPos = in.position();
        int savedLimit = in.limit();
        in.limit(batchEnd);
        crc.update(in);
        in.limit(savedLimit);
        in.position(savedPos);
        int crcComputed = (int) crc.getValue();
        if (crcComputed != crcRead) {
            throw new IllegalArgumentException(
                    "CRC mismatch: computed=" + crcComputed + " read=" + crcRead + " at batch start=" + start);
        }

        short attributes = in.getShort();
        int lastOffsetDelta = in.getInt();
        long firstTimestamp = in.getLong();
        long maxTimestamp = in.getLong();
        long producerId = in.getLong();
        short producerEpoch = in.getShort();
        int baseSequence = in.getInt();
        int numRecords = in.getInt();

        var records = new ArrayList<Record>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            int inner = Varints.readVarint(in);
            int recEnd = in.position() + inner;
            in.get(); // attributes byte (unused)
            long timestampDelta = Varints.readVarlong(in);
            int offsetDelta = Varints.readVarint(in);
            int keyLen = Varints.readVarint(in);
            byte[] key = null;
            if (keyLen >= 0) {
                key = new byte[keyLen];
                in.get(key);
            }
            int valueLen = Varints.readVarint(in);
            byte[] value = null;
            if (valueLen >= 0) {
                value = new byte[valueLen];
                in.get(value);
            }
            int headerCount = Varints.readVarint(in);
            byte[][] headers;
            if (headerCount <= 0) {
                headers = Record.NO_HEADERS;
            } else {
                headers = new byte[headerCount * 2][];
                for (int h = 0; h < headerCount; h++) {
                    int hkLen = Varints.readVarint(in);
                    byte[] hk = null;
                    if (hkLen >= 0) {
                        hk = new byte[hkLen];
                        in.get(hk);
                    }
                    int hvLen = Varints.readVarint(in);
                    byte[] hv = null;
                    if (hvLen >= 0) {
                        hv = new byte[hvLen];
                        in.get(hv);
                    }
                    headers[h * 2] = hk;
                    headers[h * 2 + 1] = hv;
                }
            }
            in.position(recEnd); // skip any unread trailer
            records.add(new Record(offsetDelta, timestampDelta, key, value, headers));
        }
        if (attributes != 0) {
            /* Compression / transactional bits not yet supported; tolerate
             * zero-valued attributes and ignore non-zero for now. */
        }
        return new Parsed(
                baseOffset,
                batchLength,
                partitionLeaderEpoch,
                lastOffsetDelta,
                firstTimestamp,
                maxTimestamp,
                producerId,
                producerEpoch,
                baseSequence,
                records);
    }
}
