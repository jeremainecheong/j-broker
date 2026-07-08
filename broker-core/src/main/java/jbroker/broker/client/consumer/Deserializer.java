package jbroker.broker.client.consumer;

/**
 * Decode raw bytes from the broker into typed values. Built-in
 * implementations: {@link ByteArrayDeserializer} (identity) and
 * {@link StringDeserializer} (UTF-8). Callers can implement this interface
 * for application types.
 */
@FunctionalInterface
public interface Deserializer<T> {
    T deserialize(byte[] bytes);
}
