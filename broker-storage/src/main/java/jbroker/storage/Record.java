package jbroker.storage;

/**
 * A single Kafka-v2-style record inside a {@link RecordBatch}. Headers are
 * not yet supported — {@link #headers} always returns an empty array.
 *
 * @param offsetDelta offset relative to the batch's baseOffset
 * @param timestampDelta milliseconds relative to the batch's firstTimestamp
 * @param key may be {@code null}
 * @param value may be {@code null}
 */
public record Record(int offsetDelta, long timestampDelta, byte[] key, byte[] value) {

    public static final Record[] NO_HEADERS = new Record[0];

    public Record {
        if (offsetDelta < 0) throw new IllegalArgumentException("offsetDelta must be non-negative");
    }

    public Record(int offsetDelta, long timestampDelta, byte[] value) {
        this(offsetDelta, timestampDelta, null, value);
    }

    public byte[][] headers() {
        return new byte[0][];
    }
}
