package jbroker.app;

import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;

/**
 * P8.9 — lightweight admin CLI. Talks to the Phase 8 admin-app's REST API
 * via JDK {@link HttpClient} (no new dependency). Intentionally minimal:
 * status-code + raw-body output, no JSON pretty-printing. For interactive
 * exploration the UI is a better fit; this CLI exists to cover the
 * DoD-listed admin operations in scripts.
 *
 * <pre>
 *   j-broker admin [--admin URL] cluster-info
 *   j-broker admin [--admin URL] topics list
 *   j-broker admin [--admin URL] topics describe --topic NAME
 *   j-broker admin [--admin URL] topics create --topic NAME [--partitions N] [--rf N]
 *   j-broker admin [--admin URL] topics delete --topic NAME
 *   j-broker admin [--admin URL] groups list
 *   j-broker admin [--admin URL] groups describe --group ID
 *   j-broker admin [--admin URL] raft
 * </pre>
 */
public final class AdminCli {

    private AdminCli() {}

    public static void run(String[] args) {
        if (args.length == 0) {
            usage(System.err);
            return;
        }
        String adminUrl = flag(args, "--admin", "http://localhost:9090");
        String[] rest = stripKnownFlags(args, "--admin");
        switch (rest[0]) {
            case "cluster-info" -> get(adminUrl + "/api/v1/cluster");
            case "topics" -> topics(adminUrl, Arrays.copyOfRange(rest, 1, rest.length));
            case "groups" -> groups(adminUrl, Arrays.copyOfRange(rest, 1, rest.length));
            case "raft" -> get(adminUrl + "/api/v1/raft");
            default -> usage(System.err);
        }
    }

    private static void topics(String adminUrl, String[] args) {
        if (args.length == 0) {
            usage(System.err);
            return;
        }
        switch (args[0]) {
            case "list" -> get(adminUrl + "/api/v1/topics");
            case "describe" -> {
                var t = flag(args, "--topic", null);
                if (t == null) {
                    System.err.println("--topic required");
                    return;
                }
                get(adminUrl + "/api/v1/topics/" + t);
            }
            case "create" -> {
                var t = flag(args, "--topic", null);
                int p = Integer.parseInt(flag(args, "--partitions", "1"));
                int rf = Integer.parseInt(flag(args, "--rf", "1"));
                if (t == null) {
                    System.err.println("--topic required");
                    return;
                }
                // P11.7 — admin REST envelope is snake_case.
                postJson(
                        adminUrl + "/api/v1/topics",
                        String.format(
                                "{\"name\":\"%s\",\"partitions\":%d,\"replication_factor\":%d,\"config\":{}}",
                                t, p, rf));
            }
            case "delete" -> {
                var t = flag(args, "--topic", null);
                if (t == null) {
                    System.err.println("--topic required");
                    return;
                }
                delete(adminUrl + "/api/v1/topics/" + t);
            }
            default -> usage(System.err);
        }
    }

    private static void groups(String adminUrl, String[] args) {
        if (args.length == 0) {
            usage(System.err);
            return;
        }
        switch (args[0]) {
            case "list" -> get(adminUrl + "/api/v1/consumer-groups");
            case "describe" -> {
                var g = flag(args, "--group", null);
                if (g == null) {
                    System.err.println("--group required");
                    return;
                }
                get(adminUrl + "/api/v1/consumer-groups/" + g);
            }
            default -> usage(System.err);
        }
    }

    private static void get(String url) {
        send(HttpRequest.newBuilder(URI.create(url)).GET().build());
    }

    private static void delete(String url) {
        send(HttpRequest.newBuilder(URI.create(url)).DELETE().build());
    }

    private static void postJson(String url, String body) {
        send(HttpRequest.newBuilder(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build());
    }

    private static void send(HttpRequest req) {
        try {
            var client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .build();
            var resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            System.out.println("HTTP " + resp.statusCode());
            if (!resp.body().isEmpty()) System.out.println(resp.body());
            if (resp.statusCode() >= 400) System.exit(1);
        } catch (Exception e) {
            System.err.println("request failed: " + e.getMessage());
            System.exit(1);
        }
    }

    private static String flag(String[] args, String name, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) return args[i + 1];
        }
        return defaultValue;
    }

    /** Returns a copy of {@code args} with every occurrence of {@code flag} and its value removed. */
    private static String[] stripKnownFlags(String[] args, String flag) {
        var out = new java.util.ArrayList<String>(args.length);
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(flag) && i + 1 < args.length) {
                i++;
                continue;
            }
            out.add(args[i]);
        }
        return out.toArray(new String[0]);
    }

    private static void usage(PrintStream out) {
        out.println("Usage: j-broker admin [--admin URL] <subcommand>");
        out.println("  cluster-info");
        out.println("  topics list|describe|create|delete ...");
        out.println("  groups list|describe --group ID");
        out.println("  raft");
    }
}
