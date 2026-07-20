package jbroker.broker.auth;

import java.util.Locale;

/**
 * Cluster-wide client authentication mode.
 *
 * <ul>
 *   <li>{@link #NONE} — every client is anonymous. The broker still
 *       extracts a principal when the transport happens to carry a client
 *       certificate, but nothing is rejected. The default; startup logs a
 *       warning so an exposed deployment can't miss it.
 *   <li>{@link #MTLS} — the principal is the CN of the mTLS client
 *       certificate. RPCs arriving without one are rejected before any
 *       handler runs. Requires TLS on the broker listener.
 * </ul>
 */
public enum AuthMode {
    NONE,
    MTLS;

    /** Parse a config value ({@code none} / {@code mtls}), case-insensitive. */
    public static AuthMode parse(String value) {
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "none" -> NONE;
            case "mtls" -> MTLS;
            default -> throw new IllegalArgumentException("unknown auth.mode: " + value + " (expected none or mtls)");
        };
    }
}
