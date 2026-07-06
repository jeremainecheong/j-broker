package jbroker.app.testkit;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;

/**
 * Bounded retry for the classic {@code freePort()} → bind TOCTOU race:
 * another process can grab a port between the probe socket's close and the
 * server's own bind. First root-caused for HighVolumeSmokeTest in #97;
 * extracted here after {@code E2E_9_3_ChaosKillBrokerIT} hit the same race
 * on main (run 24900695908).
 *
 * <p>Retries must be <b>whole-attempt</b>: an attempt allocates all fresh
 * ports itself and starts everything that depends on them. Per-broker
 * retry with a fresh port does not compose with multi-broker clusters,
 * whose static voter lists bake every peer's port into every other
 * broker's config. See {@link TestBrokerCluster} for the multi-broker
 * wrapper that also handles partial-start cleanup.
 *
 * <p>5 attempts makes the flake probability (p ≈ 1e-3 on a laptop, up to
 * 1e-2 on shared CI) ^5 — negligible. A bind race that persists across 5
 * attempts is a real networking problem and is rethrown as such.
 */
public final class BindRetry {

    private static final int DEFAULT_ATTEMPTS = 5;

    private BindRetry() {}

    /** One whole start attempt; allocate fresh ports inside the lambda. */
    @FunctionalInterface
    public interface Attempt<T> {
        T run() throws Exception;
    }

    public static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@code n} ephemeral ports. Distinct in practice; each probe socket is closed before return. */
    public static int[] freePorts(int n) {
        var ports = new int[n];
        for (int i = 0; i < n; i++) ports[i] = freePort();
        return ports;
    }

    public static <T> T startWithBindRetry(Attempt<T> attempt) throws Exception {
        return startWithBindRetry(DEFAULT_ATTEMPTS, attempt);
    }

    public static <T> T startWithBindRetry(int attempts, Attempt<T> attempt) throws Exception {
        Exception lastFailure = null;
        for (int i = 0; i < attempts; i++) {
            try {
                return attempt.run();
            } catch (Exception e) {
                if (!isBindRace(e)) throw e;
                lastFailure = e;
            }
        }
        throw new AssertionError(
                "bind race persisted across " + attempts + " attempts — real networking issue, not a flake",
                lastFailure);
    }

    /**
     * True for bind-time failures that can be retried with a fresh port:
     * {@link BindException}, Netty's {@code NativeIoException} from a
     * failed bind, or anything in the cause chain whose message says
     * "Address already in use" / "bind".
     */
    public static boolean isBindRace(Throwable t) {
        while (t != null) {
            if (t instanceof BindException) return true;
            if (t.getClass().getName().contains("NativeIoException")) {
                String msg = t.getMessage();
                if (msg != null && (msg.contains("Address already in use") || msg.contains("bind"))) return true;
            }
            t = t.getCause();
        }
        return false;
    }
}
