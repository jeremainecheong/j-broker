package jbroker.broker.chaos;

import io.grpc.Metadata;

/**
 * gRPC metadata headers used by the chaos interceptors to
 * identify the calling broker. Both {@link ChaosClientInterceptor} (adds)
 * and {@link ChaosServerInterceptor} (reads) reference the same key
 * instance so there's one source of truth for the wire name.
 */
public final class ChaosMetadataKeys {

    public static final Metadata.Key<String> FROM_BROKER_ID =
            Metadata.Key.of("jbroker-from-broker-id", Metadata.ASCII_STRING_MARSHALLER);

    private ChaosMetadataKeys() {}
}
