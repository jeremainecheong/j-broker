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

    static boolean has(String[] args, String name) {
        for (var a : args) if (a.equals(name)) return true;
        return false;
    }
}
