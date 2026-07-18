package jbroker.app;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * Layered server configuration: defaults ← {@code j-broker.yaml} ←
 * {@code JBROKER_*} environment variables ← command-line flags. Every key
 * is declared once in {@link #KEYS} with its default and description — the
 * same table drives validation, the {@code --validate-config} report, and
 * the generated configuration reference, so none of the three can drift.
 *
 * <p>The YAML file is a flat map of dotted keys to scalars. Unknown keys
 * in the file are startup errors (they are almost always typos of real
 * keys); unknown {@code JBROKER_*} environment variables only warn, since
 * the environment is a shared namespace. Env names map as
 * {@code storage.headroom.bytes → JBROKER_STORAGE_HEADROOM_BYTES}.
 */
final class ServerConfig {

    /** One declared config key. {@code flag} is the CLI override, null when flag-less. */
    record Key(String name, String flag, String defaultValue, String description) {}

    static final String ENV_PREFIX = "JBROKER_";

    static final List<Key> KEYS = List.of(
            new Key("node.id", "--id", "1", "Broker id. Must appear in the voter list."),
            new Key("data.dir", "--data-dir", "./var/broker", "Data directory (Raft log + partition logs)."),
            new Key("broker.port", "--broker-port", "9092", "Client and inter-broker gRPC port."),
            new Key("raft.port", "--raft-port", "9192", "Raft peer RPC port."),
            new Key(
                    "voters",
                    "--voters",
                    "",
                    "Cluster voter list, `ID@HOST:RAFT:BROKER,...`. Empty runs a single-broker "
                            + "cluster with self as the only voter."),
            new Key(
                    "advertised.listeners",
                    "--advertised-listeners",
                    "",
                    "Client-facing address overlay, `ID=HOST:PORT,...`. Ids absent from the overlay "
                            + "advertise their bind address."),
            new Key("chaos.port", "--chaos-port", "-1", "Cooperative chaos HTTP port. -1 disables."),
            new Key(
                    "consumer.offsets.partitions",
                    "--consumer-offsets-partitions",
                    String.valueOf(jbroker.broker.ConsumerOffsetsTopic.PARTITION_COUNT),
                    "Partition count for the internal `__consumer_offsets` topic. Fixed at first boot."),
            new Key(
                    "min.insync.replicas",
                    "--min-insync-replicas",
                    String.valueOf(jbroker.broker.ProduceHandler.DEFAULT_MIN_INSYNC_REPLICAS),
                    "Cluster default acks=all durability floor. Per-topic config overrides; RF-1 "
                            + "topics clamp down."),
            new Key(
                    "max.message.bytes",
                    null,
                    String.valueOf(jbroker.broker.ProduceHandler.DEFAULT_MAX_MESSAGE_BYTES),
                    "Cluster default for the largest serialized produce batch. Per-topic config "
                            + "overrides; hard cap 8 MiB (gRPC frame limits bound every hop)."),
            new Key(
                    "log.segment.bytes",
                    null,
                    "134217728",
                    "Cluster default segment roll threshold. Per-topic `segment.bytes` overrides."),
            new Key(
                    "log.retention.ms",
                    null,
                    "604800000",
                    "Cluster default time retention (7 days). -1 = unlimited. Per-topic "
                            + "`retention.ms` overrides."),
            new Key(
                    "log.retention.bytes",
                    null,
                    "-1",
                    "Cluster default size-retention budget per partition. -1 = unlimited. "
                            + "Per-topic `retention.bytes` overrides."),
            new Key(
                    "log.flush.messages",
                    null,
                    "-1",
                    "Cluster default flush count trigger. -1 = off (fsync on segment roll + "
                            + "replication). Per-topic `flush.messages` overrides."),
            new Key(
                    "log.flush.ms",
                    null,
                    "-1",
                    "Cluster default flush age trigger, ms. -1 = off. Per-topic `flush.ms` overrides."),
            new Key("log.cleaner.interval.ms", null, "300000", "Retention/compaction cleaner tick interval, ms."),
            new Key(
                    "offsets.retention.ms",
                    null,
                    "604800000",
                    "Committed offsets of groups with no live members expire once their newest "
                            + "commit is older than this (7 days). -1 disables expiry."),
            new Key(
                    "shutdown.timeout.ms",
                    null,
                    "30000",
                    "SIGTERM drain budget: how long the broker spends handing led partitions to "
                            + "other ISR members before closing. 0 skips the drain."),
            new Key(
                    "storage.headroom.bytes",
                    null,
                    String.valueOf(jbroker.broker.DiskHeadroom.DEFAULT_HEADROOM_BYTES),
                    "Disk-headroom watermark. Below it, client produces get retriable STORAGE_FULL "
                            + "while fetch/replication/admin keep serving."),
            new Key("tls.enabled", null, "false", "Enable mTLS on every gRPC listener and inter-broker client."),
            new Key("tls.cert", "--tls-cert", "", "PEM certificate chain. Required when tls.enabled."),
            new Key("tls.key", "--tls-key", "", "PEM private key. Required when tls.enabled."),
            new Key(
                    "tls.trust",
                    "--tls-trust",
                    "",
                    "PEM trust bundle peers are verified against. Required when " + "tls.enabled."));

    /** Resolved value + where it came from, for the --validate-config report. */
    record Resolved(String value, String source) {}

    private final Map<String, Resolved> values;

    private ServerConfig(Map<String, Resolved> values) {
        this.values = values;
    }

    /**
     * Layer defaults ← file ← env ← flags. {@code configFile} may be null
     * (no file). Throws {@link IllegalArgumentException} with every
     * problem found, not just the first.
     */
    static ServerConfig resolve(Path configFile, Map<String, String> env, String[] flags) throws IOException {
        var errors = new ArrayList<String>();
        var out = new LinkedHashMap<String, Resolved>();
        for (var key : KEYS) {
            out.put(key.name(), new Resolved(key.defaultValue(), "default"));
        }

        if (configFile != null) {
            for (var e : loadYaml(configFile, errors).entrySet()) {
                if (!out.containsKey(e.getKey())) {
                    errors.add("unknown key in " + configFile + ": " + e.getKey());
                    continue;
                }
                out.put(e.getKey(), new Resolved(e.getValue(), configFile.toString()));
            }
        }

        for (var key : KEYS) {
            String envName = ENV_PREFIX + key.name().replace('.', '_').toUpperCase(java.util.Locale.ROOT);
            String envValue = env.get(envName);
            if (envValue != null) {
                out.put(key.name(), new Resolved(envValue, "env " + envName));
            }
        }
        warnUnknownEnv(env);

        for (var key : KEYS) {
            if (key.flag() == null) continue;
            String flagValue = flagValue(flags, key.flag());
            if (flagValue != null) {
                out.put(key.name(), new Resolved(flagValue, "flag " + key.flag()));
            }
        }
        // --tls-enabled is a switch, not a valued flag.
        for (var a : flags) {
            if (a.equals("--tls-enabled")) {
                out.put("tls.enabled", new Resolved("true", "flag --tls-enabled"));
            }
        }

        if (!errors.isEmpty()) {
            throw new IllegalArgumentException(String.join("\n", errors));
        }
        return new ServerConfig(out);
    }

    private static Map<String, String> loadYaml(Path file, List<String> errors) throws IOException {
        if (!Files.isRegularFile(file)) {
            errors.add("config file not found: " + file);
            return Map.of();
        }
        Object root;
        try {
            root = new Yaml(new SafeConstructor(new LoaderOptions())).load(Files.readString(file));
        } catch (RuntimeException e) {
            errors.add("config file " + file + " is not valid YAML: " + e.getMessage());
            return Map.of();
        }
        if (root == null) return Map.of();
        if (!(root instanceof Map<?, ?> map)) {
            errors.add("config file " + file + " must be a flat map of dotted keys, got: "
                    + root.getClass().getSimpleName());
            return Map.of();
        }
        var out = new LinkedHashMap<String, String>();
        for (var e : map.entrySet()) {
            if (e.getValue() instanceof Map || e.getValue() instanceof List) {
                errors.add("config key '" + e.getKey() + "' must be a scalar (flat dotted keys only)");
                continue;
            }
            out.put(String.valueOf(e.getKey()), e.getValue() == null ? "" : String.valueOf(e.getValue()));
        }
        return out;
    }

    private static void warnUnknownEnv(Map<String, String> env) {
        var known = new java.util.HashSet<String>();
        for (var key : KEYS) {
            known.add(ENV_PREFIX + key.name().replace('.', '_').toUpperCase(java.util.Locale.ROOT));
        }
        for (var name : env.keySet()) {
            if (name.startsWith(ENV_PREFIX) && !known.contains(name)) {
                System.err.println("warning: ignoring unrecognized environment variable " + name);
            }
        }
    }

    private static String flagValue(String[] flags, String name) {
        for (int i = 0; i < flags.length - 1; i++) {
            if (flags[i].equals(name)) return flags[i + 1];
        }
        return null;
    }

    // ---- Typed accessors ----

    String raw(String key) {
        var r = values.get(key);
        if (r == null) throw new IllegalArgumentException("undeclared config key: " + key);
        return r.value();
    }

    int intValue(String key) {
        try {
            return Integer.parseInt(raw(key).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be an integer, got: " + raw(key));
        }
    }

    long longValue(String key) {
        try {
            return Long.parseLong(raw(key).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be an integer, got: " + raw(key));
        }
    }

    boolean boolValue(String key) {
        var v = raw(key).trim();
        if (v.equalsIgnoreCase("true")) return true;
        if (v.equalsIgnoreCase("false")) return false;
        throw new IllegalArgumentException(key + " must be true or false, got: " + v);
    }

    /**
     * Static validation of every key: types parse, ranges hold, the TLS
     * triple is complete and points at existing files when enabled, and
     * the voter list (when present) parses and contains node.id. Returns
     * every problem found; empty means valid.
     */
    List<String> validate() {
        var errors = new ArrayList<String>();
        checkInt(errors, "node.id", 1, Integer.MAX_VALUE);
        checkInt(errors, "broker.port", 1, 65535);
        checkInt(errors, "raft.port", 1, 65535);
        checkInt(errors, "chaos.port", -1, 65535);
        checkInt(errors, "consumer.offsets.partitions", 1, Integer.MAX_VALUE);
        checkInt(errors, "min.insync.replicas", 1, Integer.MAX_VALUE);
        checkInt(errors, "max.message.bytes", 1, jbroker.broker.TopicDescription.MAX_MESSAGE_BYTES_HARD_CAP);
        checkLong(
                errors,
                "log.segment.bytes",
                jbroker.broker.TopicDescription.SEGMENT_BYTES_FLOOR,
                jbroker.broker.TopicDescription.SEGMENT_BYTES_HARD_CAP);
        checkMinusOneOrPositive(errors, "log.retention.ms");
        checkMinusOneOrPositive(errors, "log.retention.bytes");
        checkMinusOneOrPositive(errors, "log.flush.messages");
        checkMinusOneOrPositive(errors, "log.flush.ms");
        checkLong(errors, "log.cleaner.interval.ms", 1, Long.MAX_VALUE);
        checkMinusOneOrPositive(errors, "offsets.retention.ms");
        checkLong(errors, "shutdown.timeout.ms", 0, Long.MAX_VALUE);
        checkLong(errors, "storage.headroom.bytes", 1, Long.MAX_VALUE);
        if (raw("data.dir").isBlank()) {
            errors.add("data.dir must be non-blank");
        }

        try {
            if (!raw("voters").isBlank()) {
                var voters = BrokerApp.parseVoters(raw("voters"));
                int id = intValue("node.id");
                if (voters.stream().noneMatch(v -> v.id().value() == id)) {
                    errors.add("voters must include node.id (" + id + "), got: " + raw("voters"));
                }
                if (!raw("advertised.listeners").isBlank()) {
                    BrokerApp.overlayAdvertisedListeners(voters, raw("advertised.listeners"));
                }
            }
        } catch (IllegalArgumentException e) {
            errors.add(e.getMessage());
        }

        try {
            if (boolValue("tls.enabled")) {
                for (var k : new String[] {"tls.cert", "tls.key", "tls.trust"}) {
                    if (raw(k).isBlank()) {
                        errors.add(k + " is required when tls.enabled=true");
                    } else if (!Files.isRegularFile(Path.of(raw(k)))) {
                        errors.add(k + " does not exist: " + raw(k));
                    }
                }
            }
        } catch (IllegalArgumentException e) {
            errors.add(e.getMessage());
        }
        return errors;
    }

    private void checkInt(List<String> errors, String key, int min, int max) {
        try {
            int v = intValue(key);
            if (v < min || v > max) {
                errors.add(key + " must be in [" + min + ", " + max + "], got: " + v);
            }
        } catch (IllegalArgumentException e) {
            errors.add(e.getMessage());
        }
    }

    /** Retention/flush shape: -1 (unlimited / off) or a positive value. */
    private void checkMinusOneOrPositive(List<String> errors, String key) {
        try {
            long v = longValue(key);
            if (v != -1 && v < 1) {
                errors.add(key + " must be -1 or ≥ 1, got: " + v);
            }
        } catch (IllegalArgumentException e) {
            errors.add(e.getMessage());
        }
    }

    private void checkLong(List<String> errors, String key, long min, long max) {
        try {
            long v = longValue(key);
            if (v < min || v > max) {
                errors.add(key + " must be in [" + min + ", " + max + "], got: " + v);
            }
        } catch (IllegalArgumentException e) {
            errors.add(e.getMessage());
        }
    }

    /** Resolved key = value lines with provenance, for --validate-config. */
    String describe() {
        var sb = new StringBuilder();
        for (var e : values.entrySet()) {
            sb.append(e.getKey())
                    .append(" = ")
                    .append(e.getValue().value())
                    .append("   (")
                    .append(e.getValue().source())
                    .append(")\n");
        }
        return sb.toString();
    }

    /** Markdown table of every key, default, env name, flag — the configuration reference. */
    static String renderReference() {
        var sb = new StringBuilder();
        sb.append("| Key | Default | Env | Flag | Description |\n");
        sb.append("|---|---|---|---|---|\n");
        for (var key : KEYS) {
            sb.append("| `").append(key.name()).append("` ");
            sb.append("| `")
                    .append(key.defaultValue().isEmpty() ? "(empty)" : key.defaultValue())
                    .append("` ");
            sb.append("| `")
                    .append(ENV_PREFIX)
                    .append(key.name().replace('.', '_').toUpperCase(java.util.Locale.ROOT))
                    .append("` ");
            sb.append("| ")
                    .append(key.flag() == null ? "—" : "`" + key.flag() + "`")
                    .append(" ");
            sb.append("| ").append(key.description()).append(" |\n");
        }
        return sb.toString();
    }
}
