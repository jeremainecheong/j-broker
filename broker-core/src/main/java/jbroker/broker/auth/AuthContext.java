package jbroker.broker.auth;

import io.grpc.Context;
import java.util.Optional;

/**
 * Request-scoped authenticated principal, populated by
 * {@link MtlsAuthInterceptor} and read by handlers (quota accounting
 * today, ACL checks next). Rides gRPC's {@link Context} so it follows the
 * request across the handler call chain without threading a parameter
 * through every signature.
 */
public final class AuthContext {

    /** Principal accounting name used when no client identity is present. */
    public static final String ANONYMOUS = "anonymous";

    static final Context.Key<String> PRINCIPAL = Context.key("jbroker-principal");

    private AuthContext() {}

    /** The authenticated principal of the current request, if any. */
    public static Optional<String> principal() {
        return Optional.ofNullable(PRINCIPAL.get());
    }

    /** The authenticated principal, or {@link #ANONYMOUS} outside an authenticated call. */
    public static String principalOrAnonymous() {
        String p = PRINCIPAL.get();
        return p == null ? ANONYMOUS : p;
    }
}
