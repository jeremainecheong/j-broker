package jbroker.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * Kafka v2 record batch, simplified: no transactional fields. Supports
 * encode (for append) and decode (for fetch / recovery). Per-record
 * headers are optional — encoded as a varint count followed by
 * {@code (keyLen varint, key bytes, valueLen varint, value bytes)} pairs.
 *
 * <p>The low three bits of {@code attributes} carry the {@link Compression}
 * codec of the records section. Only the records bytes are compressed; the
 * 61-byte header (offsets, epochs, producer fields, CRC) stays plaintext so
 * replication, the sparse index and recovery scans keep working on raw
 * bytes. The CRC covers the bytes as stored — compressed batches replicate
 * byte-identically without decompression. {@link #decode} decompresses
 * transparently and rejects codec ids this binary does not implement.
 *
 * <p>Bit 3 ({@link #ATTR_CONTROL}) marks a control batch: exactly one
 * record, never compressed, whose value is a {@link ControlRecord}
 * transaction marker rather than application data — callers must check
 * {@link Parsed#control()} before treating records as payload. Bit 4
 * ({@link #ATTR_TRANSACTIONAL}) marks a data batch as part of the writing
 * producer's open transaction. Two separate bits because neither implies
 * the other: {@code producerId >= 0} alone already means idempotent (not
 * necessarily transactional), and a control batch is not itself
 * transactional data. Both flags live in the plaintext header so scans and
 * replication classify batches without decoding records.
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
 *           (bits 0-2: codec, bit 3: control, bit 4: transactional)
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

    /** Byte offset of the attributes field — plaintext, peekable without decode. */
    public static final int ATTRIBUTES_OFFSET = 21;

    /** Byte offset of the producerId field — plaintext, peekable without decode. */
    public static final int PRODUCER_ID_OFFSET = 43;

    /** Attributes bit marking a control batch (transaction marker, not application data). */
    public static final short ATTR_CONTROL = 0x08;

    /** Attributes bit marking a data batch written inside an open transaction. */
    public static final short ATTR_TRANSACTIONAL = 0x10;

    /** True when {@code attributes} carries the control bit. */
    public static boolean isControl(short attributes) {
        return (attributes & ATTR_CONTROL) != 0;
    }

    /** True when {@code attributes} carries the transactional bit. */
    public static boolean isTransactional(short attributes) {
        return (attributes & ATTR_TRANSACTIONAL) != 0;
    }

    private RecordBatch() {}

    /**
     * Parsed batch returned by {@link #decode}. {@code records} are always
     * decompressed; {@code codec} reports how the batch was stored so
     * re-encoding paths (produce append, compaction) can preserve it.
     *
     * <p>{@code control} is the application-facing distinction: a control
     * batch's single record is a transaction marker, not payload — callers
     * that hand records to applications must skip control batches and may
     * read the marker via {@link #controlRecord()}. {@code transactional}
     * reports the transactional data bit as stored.
     */
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
            Compression codec,
            boolean transactional,
            boolean control,
            List<Record> records) {

        public long lastOffset() {
            return baseOffset + lastOffsetDelta;
        }

        public int totalBytes() {
            return 12 + batchLength; // baseOffset(8) + batchLength(4) + rest
        }

        /**
         * The transaction marker of a control batch.
         *
         * @throws IllegalStateException if this is not a control batch
         * @throws IllegalArgumentException if the marker value is malformed
         */
        public ControlRecord controlRecord() {
            if (!control) throw new IllegalStateException("not a control batch");
            return ControlRecord.decode(records.get(0).value());
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
        return encode(
                out,
                baseOffset,
                partitionLeaderEpoch,
                firstTimestamp,
                maxTimestamp,
                producerId,
                producerEpoch,
                baseSequence,
                records,
                Compression.NONE);
    }

    /**
     * Encode a batch, compressing the records section with {@code codec}.
     * The codec is a request, not a guarantee: the compressed form is
     * stored only when it is strictly smaller than the uncompressed
     * encoding, so {@link #estimatedSize} stays a valid upper bound for
     * every codec (incompressible payloads never inflate the batch).
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
            List<Record> records,
            Compression codec) {
        return encode(
                out,
                baseOffset,
                partitionLeaderEpoch,
                firstTimestamp,
                maxTimestamp,
                producerId,
                producerEpoch,
                baseSequence,
                records,
                codec,
                /*transactional*/ false);
    }

    /**
     * Encode a data batch, additionally stamping {@link #ATTR_TRANSACTIONAL}
     * when {@code transactional} is set. A transactional batch requires
     * producer identity ({@code producerId >= 0}) — without it no control
     * batch could ever close the transaction.
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
            List<Record> records,
            Compression codec,
            boolean transactional) {
        if (records.isEmpty()) throw new IllegalArgumentException("records must be non-empty");
        if (transactional && producerId < 0) {
            throw new IllegalArgumentException("transactional batch requires producerId >= 0, got " + producerId);
        }
        short flagBits = transactional ? ATTR_TRANSACTIONAL : 0;
        int lastOffsetDelta = records.get(records.size() - 1).offsetDelta();
        if (codec != Compression.NONE) {
            byte[] raw = encodeRecordsSection(records);
            byte[] compressed = codec.compress(raw);
            if (compressed.length < raw.length) {
                return writeBatch(
                        out,
                        baseOffset,
                        partitionLeaderEpoch,
                        firstTimestamp,
                        maxTimestamp,
                        producerId,
                        producerEpoch,
                        baseSequence,
                        (short) (codec.id() | flagBits),
                        lastOffsetDelta,
                        records.size(),
                        o -> o.put(compressed));
            }
            // Compression didn't pay — fall through to the plain layout.
        }
        return writeBatch(
                out,
                baseOffset,
                partitionLeaderEpoch,
                firstTimestamp,
                maxTimestamp,
                producerId,
                producerEpoch,
                baseSequence,
                flagBits,
                lastOffsetDelta,
                records.size(),
                o -> {
                    for (var r : records) encodeRecord(o, r);
                });
    }

    /**
     * Encode a control batch: {@link #ATTR_CONTROL} set, exactly one record
     * (null key, {@code marker} as value, offsetDelta 0), never compressed.
     * The batch consumes one offset. {@code baseSequence} is stored as -1 —
     * markers do not participate in idempotent-producer sequencing.
     */
    public static int encodeControl(
            ByteBuffer out,
            long baseOffset,
            int partitionLeaderEpoch,
            long timestamp,
            long producerId,
            short producerEpoch,
            ControlRecord marker) {
        if (producerId < 0) {
            throw new IllegalArgumentException("control batch requires producerId >= 0, got " + producerId);
        }
        var record = new Record(0, 0L, null, marker.encode());
        return writeBatch(
                out,
                baseOffset,
                partitionLeaderEpoch,
                timestamp,
                timestamp,
                producerId,
                producerEpoch,
                /*baseSequence*/ -1,
                ATTR_CONTROL,
                /*lastOffsetDelta*/ 0,
                /*numRecords*/ 1,
                o -> encodeRecord(o, record));
    }

    private static int writeBatch(
            ByteBuffer out,
            long baseOffset,
            int partitionLeaderEpoch,
            long firstTimestamp,
            long maxTimestamp,
            long producerId,
            short producerEpoch,
            int baseSequence,
            short attributes,
            int lastOffsetDelta,
            int numRecords,
            java.util.function.Consumer<ByteBuffer> recordsWriter) {
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
        out.putShort(attributes);
        out.putInt(lastOffsetDelta);
        out.putLong(firstTimestamp);
        out.putLong(maxTimestamp);
        out.putLong(producerId);
        out.putShort(producerEpoch);
        out.putInt(baseSequence);
        out.putInt(numRecords);
        recordsWriter.accept(out);
        int endPos = out.position();

        // Fill in batchLength (= bytes after the batchLength field itself, so
        // from partitionLeaderEpoch through end-of-records).
        int batchLength = endPos - batchLengthPos - 4;
        out.putInt(batchLengthPos, batchLength);

        // Compute CRC-32C over [crcStart..endPos) — the bytes as stored, so
        // CRC verification never needs to decompress.
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

    /** Records section (all length-prefixed records, uncompressed) as one array. */
    private static byte[] encodeRecordsSection(List<Record> records) {
        int size = 0;
        for (var r : records) {
            int inner = sizeOfRecordInner(r);
            size += Varints.sizeOfVarint(inner) + inner;
        }
        var buf = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        for (var r : records) {
            encodeRecord(buf, r);
        }
        return buf.array();
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

    /**
     * Upper bound on encoded size; useful for buffer sizing. Valid for
     * every codec — compression is only applied when it shrinks the
     * records section, so the uncompressed size bounds the stored size.
     */
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
     * @throws IllegalArgumentException if CRC fails, magic is wrong, the
     *         buffer runs short, or a control batch violates its invariants
     *         (compressed, or record count != 1). Recovery scans treat any
     *         of these as corruption and truncate at the batch boundary.
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
        // Low bits name the codec; a reserved/unknown id throws here, after
        // the CRC proved the batch intact — a clean "this binary can't read
        // that codec" error, never garbage records. Bits 3/4 are the
        // control/transactional flags; bits above them stay ignored.
        var codec = Compression.fromAttributes(attributes);
        boolean control = isControl(attributes);
        boolean transactional = isTransactional(attributes);
        if (control && codec != Compression.NONE) {
            // Control batches are written uncompressed by construction; a
            // CRC-valid batch claiming both was written by nothing this
            // format defines — refuse rather than decompress a marker.
            throw new IllegalArgumentException("control batch must not be compressed: attributes 0x"
                    + Integer.toHexString(attributes & 0xFFFF) + " at batch start=" + start);
        }
        int lastOffsetDelta = in.getInt();
        long firstTimestamp = in.getLong();
        long maxTimestamp = in.getLong();
        long producerId = in.getLong();
        short producerEpoch = in.getShort();
        int baseSequence = in.getInt();
        int numRecords = in.getInt();
        if (control && numRecords != 1) {
            throw new IllegalArgumentException(
                    "control batch must carry exactly one record, found " + numRecords + " at batch start=" + start);
        }

        ByteBuffer rec = in;
        if (codec != Compression.NONE) {
            byte[] stored = new byte[batchEnd - in.position()];
            in.get(stored);
            rec = ByteBuffer.wrap(codec.decompress(stored)).order(ByteOrder.BIG_ENDIAN);
        }

        var records = new ArrayList<Record>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            int inner = Varints.readVarint(rec);
            int recEnd = rec.position() + inner;
            rec.get(); // attributes byte (unused)
            long timestampDelta = Varints.readVarlong(rec);
            int offsetDelta = Varints.readVarint(rec);
            int keyLen = Varints.readVarint(rec);
            byte[] key = null;
            if (keyLen >= 0) {
                key = new byte[keyLen];
                rec.get(key);
            }
            int valueLen = Varints.readVarint(rec);
            byte[] value = null;
            if (valueLen >= 0) {
                value = new byte[valueLen];
                rec.get(value);
            }
            int headerCount = Varints.readVarint(rec);
            byte[][] headers;
            if (headerCount <= 0) {
                headers = Record.NO_HEADERS;
            } else {
                headers = new byte[headerCount * 2][];
                for (int h = 0; h < headerCount; h++) {
                    int hkLen = Varints.readVarint(rec);
                    byte[] hk = null;
                    if (hkLen >= 0) {
                        hk = new byte[hkLen];
                        rec.get(hk);
                    }
                    int hvLen = Varints.readVarint(rec);
                    byte[] hv = null;
                    if (hvLen >= 0) {
                        hv = new byte[hvLen];
                        rec.get(hv);
                    }
                    headers[h * 2] = hk;
                    headers[h * 2 + 1] = hv;
                }
            }
            rec.position(recEnd); // skip any unread trailer
            records.add(new Record(offsetDelta, timestampDelta, key, value, headers));
        }
        in.position(batchEnd);
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
                codec,
                transactional,
                control,
                records);
    }
}
