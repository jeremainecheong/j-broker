package jbroker.storage;

/**
 * A single Kafka-v2-style record inside a {@link RecordBatch}. Headers are
 * laid out as an even-length flat array of alternating {@code key, value}
 * byte[] entries; {@link #NO_HEADERS} is the canonical empty value.
 *
 * @param offsetDelta offset relative to the batch's baseOffset
 * @param timestampDelta milliseconds relative to the batch's firstTimestamp
 * @param key may be {@code null}
 * @param value may be {@code null}
 * @param headers flat {@code [k0, v0, k1, v1, ...]} pairs; never {@code null}
 */
public record Record(int offsetDelta, long timestampDelta, byte[] key, byte[] value, byte[][] headers) {

    public static final byte[][] NO_HEADERS = new byte[0][];

    public Record {
        if (offsetDelta < 0) throw new IllegalArgumentException("offsetDelta must be non-negative");
        if (headers == null) headers = NO_HEADERS;
        if ((headers.length & 1) != 0) {
            throw new IllegalArgumentException("headers must be key/value pairs, got odd length " + headers.length);
        }
    }

    public Record(int offsetDelta, long timestampDelta, byte[] key, byte[] value) {
        this(offsetDelta, timestampDelta, key, value, NO_HEADERS);
    }

    public Record(int offsetDelta, long timestampDelta, byte[] value) {
        this(offsetDelta, timestampDelta, null, value, NO_HEADERS);
    }
}
