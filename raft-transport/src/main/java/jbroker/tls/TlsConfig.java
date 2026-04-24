package jbroker.tls;

import java.nio.file.Path;
import java.util.Objects;

/**
 * P15.2 — opt-in TLS configuration for one side of a gRPC hop. Used by
 * broker servers (server-side mTLS), inter-broker peer clients (Raft +
 * replica + cluster), and every external client (producer, consumer,
 * admin-app, admin CLI).
 *
 * <p>Formats are PEM (openssl-native) rather than JKS/PKCS12 so the dev
 * {@code scripts/tls/bootstrap-ca.sh} script stays simple and produces
 * artifacts directly consumable by gRPC's {@code SslContext} builders.
 *
 * <p>{@link #DISABLED} is the plaintext default that preserves every
 * pre-P15.2 test fixture — broker Configs and client constructors without
 * an explicit {@code TlsConfig} all resolve to this, so existing call sites
 * keep their plaintext behavior.
 *
 * <p>Record fields:
 *
 * <ul>
 *   <li>{@code enabled} — master toggle; when false, every other field is
 *       ignored and the caller should build a plaintext gRPC endpoint.
 *   <li>{@code certChain} — PEM-encoded X.509 cert chain this side presents
 *       to the peer. On broker servers: the server cert. On clients: the
 *       client cert (mTLS) or null (server-only TLS).
 *   <li>{@code privateKey} — PEM-encoded PKCS#8 private key matching
 *       {@code certChain}. Required whenever {@code certChain} is set.
 *   <li>{@code trustStore} — PEM-encoded X.509 CA bundle used to verify the
 *       peer. Always required when {@code enabled} is true.
 *   <li>{@code clientAuthRequired} — server-side only: when true, the server
 *       aborts the handshake if the client fails to present a trusted cert
 *       (mTLS). Client-side instances leave this at the default {@code false}.
 * </ul>
 */
public record TlsConfig(boolean enabled, Path certChain, Path privateKey, Path trustStore, boolean clientAuthRequired) {

    public static final TlsConfig DISABLED = new TlsConfig(false, null, null, null, false);

    public TlsConfig {
        if (enabled) {
            Objects.requireNonNull(trustStore, "trustStore is required when TLS is enabled");
            // certChain + privateKey are optional on the client side when the
            // server doesn't demand mTLS, but if one is set the other must be.
            if ((certChain == null) != (privateKey == null)) {
                throw new IllegalArgumentException("certChain and privateKey must be set together (got cert="
                        + certChain + ", key=" + privateKey + ")");
            }
            if (clientAuthRequired && certChain == null) {
                throw new IllegalArgumentException("client-auth required but no server certChain supplied");
            }
        }
    }

    /** Build a server TLS config demanding mTLS (server cert + client-auth=REQUIRE). */
    public static TlsConfig mtlsServer(Path certChain, Path privateKey, Path trustStore) {
        return new TlsConfig(true, certChain, privateKey, trustStore, true);
    }

    /** Build a client TLS config presenting a client cert (mTLS). */
    public static TlsConfig mtlsClient(Path certChain, Path privateKey, Path trustStore) {
        return new TlsConfig(true, certChain, privateKey, trustStore, false);
    }
}
