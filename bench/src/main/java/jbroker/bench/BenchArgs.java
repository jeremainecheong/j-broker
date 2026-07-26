package jbroker.bench;

/**
 * Tiny flag-parser shared by the producer and consumer perf tests. Keeps
 * CLI parsing out of the hot path so the benchmarks themselves stay
 * focused on measurement.
 */
final class BenchArgs {

    private BenchArgs() {}

    static String get(String[] args, String name, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) return args[i + 1];
        }
        return defaultValue;
    }

    static int getInt(String[] args, String name, int defaultValue) {
        var s = get(args, name, null);
        return s == null ? defaultValue : Integer.parseInt(s);
    }

    static long getLong(String[] args, String name, long defaultValue) {
        var s = get(args, name, null);
        return s == null ? defaultValue : Long.parseLong(s);
    }

    /** Every value of a repeatable flag, in argument order. */
    static java.util.List<String> getAll(String[] args, String name) {
        var values = new java.util.ArrayList<String>();
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) values.add(args[i + 1]);
        }
        return values;
    }

    static boolean has(String[] args, String name) {
        for (var a : args) if (a.equals(name)) return true;
        return false;
    }

    /**
     * Resolve optional mTLS flags into a {@link jbroker.tls.TlsConfig}.
     * Accepts {@code --tls-trust CA.crt}, {@code --tls-cert CLIENT.crt},
     * {@code --tls-key CLIENT.key}. Returns {@link jbroker.tls.TlsConfig#DISABLED}
     * if no flags are present — the bench stays plaintext by default.
     */
    static jbroker.tls.TlsConfig tlsFromArgs(String[] args) {
        String trust = get(args, "--tls-trust", null);
        if (trust == null) return jbroker.tls.TlsConfig.DISABLED;
        String cert = get(args, "--tls-cert", null);
        String key = get(args, "--tls-key", null);
        if ((cert == null) != (key == null)) {
            throw new IllegalArgumentException("--tls-cert and --tls-key must be set together");
        }
        var trustPath = java.nio.file.Path.of(trust);
        if (cert == null) {
            // Server-auth only (no client cert presented).
            return new jbroker.tls.TlsConfig(true, null, null, trustPath, false);
        }
        return jbroker.tls.TlsConfig.mtlsClient(java.nio.file.Path.of(cert), java.nio.file.Path.of(key), trustPath);
    }
}
