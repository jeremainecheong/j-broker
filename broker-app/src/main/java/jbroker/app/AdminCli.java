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
 * Lightweight admin CLI. Talks to the admin-app's REST API
 * via JDK {@link HttpClient} (no new dependency). Intentionally minimal:
 * status-code + raw-body output, no JSON pretty-printing. For interactive
 * exploration the UI is a better fit; this CLI exists to cover the
 * required admin operations in scripts.
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
 *   j-broker admin [--admin URL] cluster membership
 *   j-broker admin [--admin URL] cluster add-broker --id N --host H --raft-port N --broker-port N
 *   j-broker admin [--admin URL] cluster decommission --id N
 *   j-broker admin [--admin URL] cluster reassign --topic NAME --partition N --replicas a,b,c
 *   j-broker admin [--admin URL] cluster reassignments
 *   j-broker admin [--admin URL] cluster cancel-reassignment --topic NAME --partition N
 *   j-broker admin [--admin URL] cluster rebalance-leaders
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
            case "cluster" -> cluster(adminUrl, Arrays.copyOfRange(rest, 1, rest.length));
            case "verify-log" -> verifyLog(Arrays.copyOfRange(rest, 1, rest.length));
            default -> usage(System.err);
        }
    }

    /**
     * Offline backup integrity check: {@code j-broker admin verify-log DIR}
     * where DIR is a broker data dir (or its {@code topics/} subdirectory).
     * CRC-verifies every batch of every partition without touching the
     * files; exits 1 if anything fails to decode, so restore scripts can
     * gate on it.
     */
    private static void verifyLog(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: j-broker admin verify-log DIR");
            System.exit(2);
            return;
        }
        var dir = java.nio.file.Path.of(args[0]);
        // Accept the data dir itself for convenience; the logs live under topics/.
        var topicsDir = java.nio.file.Files.isDirectory(dir.resolve("topics")) ? dir.resolve("topics") : dir;
        try {
            var reports = jbroker.storage.LogVerifier.verify(topicsDir);
            for (var r : reports) {
                if (r.problem() == null) {
                    System.out.printf(
                            "OK      %s  segments=%d batches=%d records=%d nextOffset=%d%n",
                            r.partitionDir(), r.segments(), r.batches(), r.records(), r.nextOffset());
                } else {
                    System.out.printf("CORRUPT %s  %s%n", r.partitionDir(), r.problem());
                }
            }
            if (!jbroker.storage.LogVerifier.allClean(reports)) {
                System.exit(1);
            }
            System.out.println(reports.size() + " partition(s) verified clean");
        } catch (java.io.IOException e) {
            System.err.println("verify-log failed: " + e.getMessage());
            System.exit(2);
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
                // Admin REST envelope is snake_case.
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

    /**
     * Cluster-lifecycle verbs, routed through the admin REST layer's
     * {@code /api/v1/cluster/*} endpoints. Mutations answer 202 — joins,
     * drains, and reassignments advance asynchronously on the controller;
     * poll {@code cluster membership} / {@code cluster reassignments} to
     * watch them progress.
     */
    private static void cluster(String adminUrl, String[] args) {
        if (args.length == 0) {
            usage(System.err);
            return;
        }
        switch (args[0]) {
            case "membership" -> get(adminUrl + "/api/v1/cluster/membership");
            case "add-broker" -> {
                var id = flag(args, "--id", null);
                var host = flag(args, "--host", null);
                var raftPort = flag(args, "--raft-port", null);
                var brokerPort = flag(args, "--broker-port", null);
                if (id == null || host == null || raftPort == null || brokerPort == null) {
                    System.err.println("--id, --host, --raft-port, --broker-port required");
                    return;
                }
                // Admin REST envelope is snake_case.
                postJson(
                        adminUrl + "/api/v1/cluster/add-broker",
                        String.format(
                                "{\"broker_id\":%d,\"host\":\"%s\",\"raft_port\":%d,\"broker_port\":%d}",
                                Integer.parseInt(id), host, Integer.parseInt(raftPort), Integer.parseInt(brokerPort)));
            }
            case "decommission" -> {
                var id = flag(args, "--id", null);
                if (id == null) {
                    System.err.println("--id required");
                    return;
                }
                post(adminUrl + "/api/v1/cluster/decommission/" + Integer.parseInt(id));
            }
            case "reassign" -> {
                var t = flag(args, "--topic", null);
                var p = flag(args, "--partition", null);
                var replicas = flag(args, "--replicas", null);
                if (t == null || p == null || replicas == null) {
                    System.err.println("--topic, --partition, --replicas required");
                    return;
                }
                var ids = new StringBuilder();
                for (var r : replicas.split(",")) {
                    if (!ids.isEmpty()) ids.append(',');
                    ids.append(Integer.parseInt(r.trim()));
                }
                postJson(
                        adminUrl + "/api/v1/cluster/reassignments",
                        String.format(
                                "{\"topic\":\"%s\",\"partition\":%d,\"replicas\":[%s]}", t, Integer.parseInt(p), ids));
            }
            case "reassignments" -> get(adminUrl + "/api/v1/cluster/reassignments");
            case "cancel-reassignment" -> {
                var t = flag(args, "--topic", null);
                var p = flag(args, "--partition", null);
                if (t == null || p == null) {
                    System.err.println("--topic and --partition required");
                    return;
                }
                delete(adminUrl + "/api/v1/cluster/reassignments/" + t + "/" + Integer.parseInt(p));
            }
            case "rebalance-leaders" -> post(adminUrl + "/api/v1/cluster/rebalance-leadership");
            default -> usage(System.err);
        }
    }

    private static void get(String url) {
        send(HttpRequest.newBuilder(URI.create(url)).GET().build());
    }

    private static void post(String url) {
        send(HttpRequest.newBuilder(URI.create(url))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build());
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
        out.println("  cluster membership|reassignments|rebalance-leaders");
        out.println("  cluster add-broker --id N --host H --raft-port N --broker-port N");
        out.println("  cluster decommission --id N");
        out.println("  cluster reassign --topic NAME --partition N --replicas a,b,c");
        out.println("  cluster cancel-reassignment --topic NAME --partition N");
    }
}
