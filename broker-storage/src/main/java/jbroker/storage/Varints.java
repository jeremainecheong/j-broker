package jbroker.storage;

import java.nio.ByteBuffer;

/**
 * Protobuf-style zig-zag varint encoding, matching Kafka v2 record batch
 * semantics. Signed 32-bit values are encoded via {@code (n << 1) ^ (n >> 31)}
 * then written 7 bits at a time, MSB signalling continuation.
 */
public final class Varints {

    private Varints() {}

    public static void writeVarint(int value, ByteBuffer buf) {
        int v = (value << 1) ^ (value >> 31);
        while ((v & ~0x7F) != 0) {
            buf.put((byte) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        buf.put((byte) v);
    }

    public static int readVarint(ByteBuffer buf) {
        int raw = 0;
        int shift = 0;
        while (true) {
            byte b = buf.get();
            raw |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
            if (shift > 28) throw new IllegalArgumentException("varint too long");
        }
        return (raw >>> 1) ^ -(raw & 1);
    }

    public static void writeVarlong(long value, ByteBuffer buf) {
        long v = (value << 1) ^ (value >> 63);
        while ((v & ~0x7FL) != 0) {
            buf.put((byte) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        buf.put((byte) v);
    }

    public static long readVarlong(ByteBuffer buf) {
        long raw = 0;
        int shift = 0;
        while (true) {
            byte b = buf.get();
            raw |= ((long) (b & 0x7F)) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
            if (shift > 63) throw new IllegalArgumentException("varlong too long");
        }
        return (raw >>> 1) ^ -(raw & 1);
    }

    public static int sizeOfVarint(int value) {
        long v = ((long) value << 1) ^ ((long) value >> 31);
        int count = 1;
        while ((v & ~0x7FL) != 0) {
            count++;
            v >>>= 7;
        }
        return count;
    }

    public static int sizeOfVarlong(long value) {
        long v = (value << 1) ^ (value >> 63);
        int count = 1;
        while ((v & ~0x7FL) != 0) {
            count++;
            v >>>= 7;
        }
        return count;
    }
}
