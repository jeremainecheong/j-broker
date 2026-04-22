package jbroker.broker.client.consumer;

import java.nio.charset.StandardCharsets;

/** UTF-8 string deserializer. */
public final class StringDeserializer implements Deserializer<String> {
    @Override
    public String deserialize(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }
}
