package jbroker.broker.client.consumer;

/** Identity deserializer — returns the bytes unchanged. */
public final class ByteArrayDeserializer implements Deserializer<byte[]> {
    @Override
    public byte[] deserialize(byte[] bytes) {
        return bytes;
    }
}
