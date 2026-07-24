package jbroker.storage;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdException;

/**
 * Batch-level compression codec, carried in the low three bits of the
 * batch attributes field (Kafka-style numbering: 0 = none, 2 = zstd;
 * ids 1/gzip and 3/lz4 stay reserved for codecs this broker does not
 * implement). Only the records section of a batch is ever compressed —
 * the 61-byte header stays plaintext so replication, recovery scans and
 * the sparse index keep working on raw bytes.
 */
public enum Compression {
    NONE(0),
    ZSTD(2);

    /** Attribute bits reserved for the codec id. */
    public static final int CODEC_MASK = 0x07;

    private final int id;

    Compression(int id) {
        this.id = id;
    }

    /** Codec id as stored in the batch attributes' low bits. */
    public int id() {
        return id;
    }

    /**
     * Codec named by the attributes field's low bits.
     *
     * @throws IllegalArgumentException for reserved or unknown codec ids —
     *         a batch written by a binary with codecs this one lacks must
     *         fail loudly, not parse compressed bytes as records.
     */
    public static Compression fromAttributes(short attributes) {
        int codecId = attributes & CODEC_MASK;
        for (var codec : values()) {
            if (codec.id == codecId) return codec;
        }
        throw new IllegalArgumentException("unsupported compression codec id " + codecId + " in batch attributes 0x"
                + Integer.toHexString(attributes & 0xFFFF));
    }

    /** Compress a records section. Identity for {@link #NONE}. */
    byte[] compress(byte[] raw) {
        return switch (this) {
            case NONE -> raw;
            case ZSTD -> Zstd.compress(raw);
        };
    }

    /**
     * Decompress a stored records section. Identity for {@link #NONE}.
     *
     * @throws IllegalArgumentException if the payload is not a valid frame
     *         for this codec. CRC-verified batches only hit this on a bug,
     *         but the failure mode must be a clear error, never garbage
     *         records.
     */
    byte[] decompress(byte[] stored) {
        switch (this) {
            case NONE:
                return stored;
            case ZSTD:
                long contentSize = Zstd.getFrameContentSize(stored);
                if (contentSize < 0 || contentSize > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("invalid zstd frame: content size " + contentSize);
                }
                try {
                    return Zstd.decompress(stored, (int) contentSize);
                } catch (ZstdException e) {
                    throw new IllegalArgumentException("zstd decompression failed: " + e.getMessage(), e);
                }
            default:
                throw new AssertionError(this);
        }
    }
}
