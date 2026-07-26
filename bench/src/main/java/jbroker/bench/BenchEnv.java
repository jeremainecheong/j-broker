package jbroker.bench;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Machine + build identity stamped onto every CSV row so a published
 * number is traceable to the commit, host, CPU and JDK that produced it.
 * Every field degrades to {@code "unknown"} rather than failing the run.
 * Fields are comma/newline-stripped — the report writes bare CSV with no
 * quoting.
 */
record BenchEnv(String gitSha, String hostname, String os, String cpuModel, String jdkVersion) {

    private static volatile BenchEnv detected;

    static BenchEnv detect() {
        var cached = detected;
        if (cached == null) {
            cached = new BenchEnv(detectGitSha(), detectHostname(), detectOs(), detectCpuModel(), detectJdkVersion());
            detected = cached;
        }
        return cached;
    }

    static String sanitize(String s) {
        if (s == null || s.isBlank()) return "unknown";
        return s.replace(',', ' ').replace('\n', ' ').replace('\r', ' ').trim();
    }

    /** {@code BENCH_GIT_SHA} wins so CI/scripts can pin the sha they checked out. */
    private static String detectGitSha() {
        String env = System.getenv("BENCH_GIT_SHA");
        if (env != null && !env.isBlank()) return sanitize(env);
        return sanitize(exec("git", "rev-parse", "--short", "HEAD"));
    }

    private static String detectHostname() {
        try {
            return sanitize(InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            return sanitize(System.getenv("HOSTNAME"));
        }
    }

    private static String detectOs() {
        return sanitize(System.getProperty("os.name") + " " + System.getProperty("os.version") + " "
                + System.getProperty("os.arch"));
    }

    private static String detectCpuModel() {
        String osName = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (osName.contains("mac")) {
            return sanitize(exec("sysctl", "-n", "machdep.cpu.brand_string"));
        }
        try {
            for (String line : Files.readAllLines(Path.of("/proc/cpuinfo"))) {
                if (line.startsWith("model name")) {
                    int colon = line.indexOf(':');
                    if (colon >= 0) return sanitize(line.substring(colon + 1));
                }
            }
        } catch (IOException e) {
            // No /proc on this platform — fall through to unknown.
        }
        return "unknown";
    }

    private static String detectJdkVersion() {
        return sanitize(System.getProperty("java.version"));
    }

    /** Small helper-command runner; any failure yields null → "unknown". */
    private static String exec(String... cmd) {
        try {
            var p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            // Output is a single short line — read fully before waiting so
            // the child can never block on a full pipe.
            byte[] out = p.getInputStream().readAllBytes();
            if (!p.waitFor(3, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                return null;
            }
            if (p.exitValue() != 0) return null;
            return new String(out, StandardCharsets.UTF_8).trim();
        } catch (IOException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
