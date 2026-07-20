package jbroker.tls;

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import javax.net.ssl.SSLException;

/**
 * {@link SslContext} factories for broker and client sides of a
 * gRPC hop. Thin wrapper over {@link GrpcSslContexts} so every call site
 * passes through the same path; keeps the plaintext fallback obvious
 * ({@code null} return when {@link TlsConfig#enabled()} is false).
 *
 * <p>Server contexts use {@code ClientAuth.REQUIRE} when
 * {@link TlsConfig#clientAuthRequired()} is set — that's the mTLS knob —
 * and {@code ClientAuth.OPTIONAL} otherwise, so a client certificate is
 * always <em>requested</em>: peers that have one hand over an identity
 * (principal extraction feeds on it) while certless peers still complete
 * the handshake. Clients with a cert + key supply a client cert for
 * mutual auth; clients without one still verify the server cert against
 * the trust store (server-only TLS).
 *
 * <p>Returning {@code null} from both factories when TLS is disabled lets
 * call sites feed the result straight into their builders via a nullable
 * {@code sslContext} slot — {@link io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder}
 * and {@link io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder} both
 * interpret a null SslContext as "plaintext".
 */
public final class TlsContexts {

    private TlsContexts() {}

    /** Build a server-side {@link SslContext}, or return {@code null} when TLS is disabled. */
    public static SslContext serverContext(TlsConfig tls) throws SSLException {
        if (tls == null || !tls.enabled()) return null;
        if (tls.certChain() == null || tls.privateKey() == null) {
            throw new IllegalArgumentException("server TLS requires certChain and privateKey");
        }
        var b = SslContextBuilder.forServer(
                        tls.certChain().toFile(), tls.privateKey().toFile())
                .trustManager(tls.trustStore().toFile())
                .clientAuth(tls.clientAuthRequired() ? ClientAuth.REQUIRE : ClientAuth.OPTIONAL);
        return GrpcSslContexts.configure(b).build();
    }

    /** Build a client-side {@link SslContext}, or return {@code null} when TLS is disabled. */
    public static SslContext clientContext(TlsConfig tls) throws SSLException {
        if (tls == null || !tls.enabled()) return null;
        var b = SslContextBuilder.forClient().trustManager(tls.trustStore().toFile());
        if (tls.certChain() != null && tls.privateKey() != null) {
            b.keyManager(tls.certChain().toFile(), tls.privateKey().toFile());
        }
        return GrpcSslContexts.configure(b).build();
    }
}
