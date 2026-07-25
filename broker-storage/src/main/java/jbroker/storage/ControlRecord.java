package jbroker.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * The single record carried by a control batch: a transaction marker that
 * decides every batch the marked producer wrote since its transaction
 * opened. Stored as the record's value ({@code key = null}) in a compact
 * fixed encoding (big-endian):
 *
 * <pre>
 *  [0]     version           int8 (= 0)
 *  [1]     type              int8 (0 = COMMIT, 1 = ABORT)
 *  [2..5]  coordinatorEpoch  int32
 * </pre>
 *
 * <p>{@code coordinatorEpoch} is the fencing epoch of the transaction
 * coordinator that wrote the marker; storage records it verbatim so a
 * marker from a zombie coordinator stays distinguishable after the fact.
 */
public record ControlRecord(Type type, int coordinatorEpoch) {

    /** Marker type. Ids are the on-disk encoding — never renumber. */
    public enum Type {
        COMMIT(0),
        ABORT(1);

        private final int id;

        Type(int id) {
            this.id = id;
        }

        public int id() {
            return id;
        }

        /**
         * Type named by an on-disk id.
         *
         * @throws IllegalArgumentException for unknown ids — a marker this
         *         binary cannot interpret must fail loudly, not default
         */
        public static Type fromId(int id) {
            for (var type : values()) {
                if (type.id == id) return type;
            }
            throw new IllegalArgumentException("unknown control record type id " + id);
        }
    }

    /** Serialized length: version(1) + type(1) + coordinatorEpoch(4). */
    public static final int ENCODED_LENGTH = 6;

    static final byte VERSION = 0;

    public ControlRecord {
        if (type == null) throw new IllegalArgumentException("type must be non-null");
    }

    /** Encode as a control record value. */
    public byte[] encode() {
        var buf = ByteBuffer.allocate(ENCODED_LENGTH).order(ByteOrder.BIG_ENDIAN);
        buf.put(VERSION);
        buf.put((byte) type.id());
        buf.putInt(coordinatorEpoch);
        return buf.array();
    }

    /**
     * Decode a control record value.
     *
     * @throws IllegalArgumentException if the value is missing, has the
     *         wrong length, an unknown version, or an unknown type id
     */
    public static ControlRecord decode(byte[] value) {
        if (value == null || value.length != ENCODED_LENGTH) {
            throw new IllegalArgumentException("control record value must be " + ENCODED_LENGTH + " bytes, found "
                    + (value == null ? "null" : value.length));
        }
        var buf = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        byte version = buf.get();
        if (version != VERSION) {
            throw new IllegalArgumentException("unsupported control record version " + version);
        }
        var type = Type.fromId(buf.get());
        return new ControlRecord(type, buf.getInt());
    }
}
