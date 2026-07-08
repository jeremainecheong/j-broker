package jbroker.app;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.tls.TlsConfig;

/**
 * {@code j-broker} top-level entry point. Two modes:
 *
 * <pre>
 *   j-broker server --data-dir DIR --broker-port P [--raft-port P2] [--id N]
 *                   [--voters ID@HOST:RAFT:BROKER,...] [--chaos-port P]
 *                   [--consumer-offsets-partitions N]
 *   j-broker topics create|list|describe|...  --broker HOST:PORT --topic T ...
 *   j-broker produce --broker HOST:PORT --topic T --partition N   (stdin = one message per line)
 *   j-broker console-consumer --broker HOST:PORT --topic T --partition N [--from-beginning]
 *   j-broker consume --broker HOST:PORT --group G --topic T [--topic T2 ...]
 * </pre>
 */
public final class BrokerApp {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            return;
        }
        switch (args[0]) {
            case "server" -> runServer(Arrays.copyOfRange(args, 1, args.length));
            case "topics" -> runTopics(Arrays.copyOfRange(args, 1, args.length));
            case "produce" -> runProduce(Arrays.copyOfRange(args, 1, args.length));
            case "console-consumer" -> runConsumer(Arrays.copyOfRange(args, 1, args.length));
            case "consume" -> runGroupConsume(Arrays.copyOfRange(args, 1, args.length));
            case "admin" -> AdminCli.run(Arrays.copyOfRange(args, 1, args.length));
            default -> usage();
        }
    }

    private static void usage() {
        System.err.println("Usage: j-broker server|topics|produce|console-consumer|consume|admin ...");
    }

    private static String flag(String[] args, String name, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) return args[i + 1];
        }
        return defaultValue;
    }

    private static boolean switchFlag(String[] args, String name) {
        for (var a : args) if (a.equals(name)) return true;
        return false;
    }

    private static void runServer(String[] args) throws Exception {
        String dataDir = flag(args, "--data-dir", "./var/broker");
        int brokerPort = Integer.parseInt(flag(args, "--broker-port", "9092"));
        int raftPort = Integer.parseInt(flag(args, "--raft-port", "9192"));
        int id = Integer.parseInt(flag(args, "--id", "1"));
        String votersSpec = flag(args, "--voters", null);
        // Advertised listeners. Format: "id=host:port,id=host:port,..."
        // Applies per broker id on top of the voter list. Only the advertised
        // entry for THIS broker gets used in our own BrokerRegistrationRecord,
        // but the leader's propose path reads every voter's advertised slot
        // to register the whole cluster in one tick; pass a matching spec on
        // each broker for consistent behavior. Absent → fall back to voter
        // host/port (the original single-network behavior).
        String advertisedListenersSpec = flag(args, "--advertised-listeners", null);
        int chaosPort = Integer.parseInt(flag(args, "--chaos-port", "-1"));
        int consumerOffsetsPartitions = Integer.parseInt(flag(
                args,
                "--consumer-offsets-partitions",
                String.valueOf(jbroker.broker.ConsumerOffsetsTopic.PARTITION_COUNT)));
        // MTLS opt-in. Enabled only when --tls-enabled is set AND the
        // required --tls-cert / --tls-key / --tls-trust all point at PEM
        // files. Absent → plaintext (the original default).
        boolean tlsEnabled = switchFlag(args, "--tls-enabled");
        String tlsCert = flag(args, "--tls-cert", null);
        String tlsKey = flag(args, "--tls-key", null);
        String tlsTrust = flag(args, "--tls-trust", null);
        TlsConfig tlsConfig = TlsConfig.DISABLED;
        if (tlsEnabled) {
            if (tlsCert == null || tlsKey == null || tlsTrust == null) {
                throw new IllegalArgumentException(
                        "--tls-enabled requires --tls-cert, --tls-key, --tls-trust (all PEM paths)");
            }
            tlsConfig = TlsConfig.mtlsServer(
                    java.nio.file.Path.of(tlsCert), java.nio.file.Path.of(tlsKey), java.nio.file.Path.of(tlsTrust));
        }

        List<VoterAddress> voters;
        if (votersSpec == null || votersSpec.isBlank()) {
            // Single-broker dev server: self is the sole voter. Preserves the
            // pre-v1.1 default so existing ./gradlew :broker-app:run flows
            // keep working.
            voters = List.of(new VoterAddress(new NodeId(id), "127.0.0.1", raftPort, brokerPort));
        } else {
            voters = parseVoters(votersSpec);
            if (voters.stream().noneMatch(v -> v.id().value() == id)) {
                throw new IllegalArgumentException("--voters must include self (id=" + id + "), got: " + votersSpec);
            }
        }
        if (advertisedListenersSpec != null && !advertisedListenersSpec.isBlank()) {
            voters = overlayAdvertisedListeners(voters, advertisedListenersSpec);
        }

        var config = new Broker.Config(
                        new NodeId(id), Path.of(dataDir), raftPort, brokerPort, voters, consumerOffsetsPartitions)
                .withChaosPort(chaosPort)
                .withTls(tlsConfig);
        var broker = Broker.start(config);
        Runtime.getRuntime().addShutdownHook(new Thread(broker::close, "broker-shutdown"));
        System.out.println("j-broker listening on " + brokerPort
                + " (id=" + id + ", raft=" + raftPort + ", data=" + dataDir
                + ", voters=" + voters.size() + (chaosPort >= 0 ? ", chaos=" + chaosPort : "")
                + (tlsConfig.enabled() ? ", tls=mTLS" : "")
                + ")");
        Thread.currentThread().join();
    }

    /**
     * Parses a voter list like {@code 1@broker1:9192:9092,2@broker2:9192:9092}.
     * {@code raftPort} and {@code brokerPort} are the ports each voter binds
     * for Raft peer RPC and broker gRPC respectively — the host:port pair the
     * rest of the cluster uses to dial them.
     *
     * <p>Validations enforced:
     *
     * <ul>
     *   <li>Entry shape must match {@code ID@HOST:RAFT:BROKER}.
     *   <li>Host must be non-blank (empty host silently misroutes the cluster).
     *   <li>All three integers (id, raftPort, brokerPort) must parse; bad ints
     *       turn into {@link IllegalArgumentException} with the full spec for
     *       easy debugging instead of a bare {@link NumberFormatException}.
     *   <li>Voter ids must be unique across the list.
     * </ul>
     *
     * <p>IPv6 literals are not supported (future work — would require
     * bracketed-host parsing {@code [::1]:9192:9092}). Tests pin this shape.
     */
    static List<VoterAddress> parseVoters(String spec) {
        var out = new ArrayList<VoterAddress>();
        Set<Integer> seenIds = new HashSet<>();
        for (var entry : spec.split(",")) {
            var trimmed = entry.trim();
            if (trimmed.isEmpty()) continue;
            int at = trimmed.indexOf('@');
            if (at <= 0 || at == trimmed.length() - 1) {
                throw new IllegalArgumentException("voter spec must be ID@HOST:RAFT:BROKER, got: " + trimmed);
            }
            int vid = parseIntOrReject(trimmed.substring(0, at), "voter id", trimmed);
            var hostPorts = trimmed.substring(at + 1).split(":");
            if (hostPorts.length != 3) {
                throw new IllegalArgumentException("voter spec must be ID@HOST:RAFT:BROKER, got: " + trimmed);
            }
            String host = hostPorts[0];
            if (host.isBlank()) {
                throw new IllegalArgumentException("voter spec host must be non-blank, got: " + trimmed);
            }
            int raftPort = parseIntOrReject(hostPorts[1], "raft port", trimmed);
            int brokerPort = parseIntOrReject(hostPorts[2], "broker port", trimmed);
            if (!seenIds.add(vid)) {
                throw new IllegalArgumentException("duplicate voter id " + vid + " in --voters spec");
            }
            out.add(new VoterAddress(new NodeId(vid), host, raftPort, brokerPort));
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("--voters must contain at least one entry");
        }
        return List.copyOf(out);
    }

    /**
     * Parse a {@code --advertised-listeners id=host:port,...} spec
     * and return a new voter list with those entries overlaid on top of
     * the bind-time list. Voter ids missing from the spec keep their bind
     * host/port as the advertised value (falls back to current behavior).
     * Malformed entries are rejected at startup with a clear message.
     */
    static List<VoterAddress> overlayAdvertisedListeners(List<VoterAddress> voters, String spec) {
        var advertised = new java.util.HashMap<Integer, String[]>();
        for (var entry : spec.split(",")) {
            var trimmed = entry.trim();
            if (trimmed.isEmpty()) continue;
            int eq = trimmed.indexOf('=');
            if (eq <= 0 || eq == trimmed.length() - 1) {
                throw new IllegalArgumentException("advertised-listeners entry must be ID=HOST:PORT, got: " + trimmed);
            }
            int vid = parseIntOrReject(trimmed.substring(0, eq), "advertised id", trimmed);
            var hp = trimmed.substring(eq + 1).split(":");
            if (hp.length != 2 || hp[0].isBlank()) {
                throw new IllegalArgumentException("advertised-listeners entry must be ID=HOST:PORT, got: " + trimmed);
            }
            int port = parseIntOrReject(hp[1], "advertised port", trimmed);
            if (advertised.putIfAbsent(vid, new String[] {hp[0], Integer.toString(port)}) != null) {
                throw new IllegalArgumentException("duplicate advertised id " + vid + " in --advertised-listeners");
            }
        }
        var out = new ArrayList<VoterAddress>(voters.size());
        for (var v : voters) {
            var ov = advertised.get(v.id().value());
            if (ov == null) {
                out.add(v);
            } else {
                out.add(new VoterAddress(
                        v.id(), v.host(), v.raftPort(), v.brokerPort(), ov[0], Integer.parseInt(ov[1])));
            }
        }
        return List.copyOf(out);
    }

    private static int parseIntOrReject(String s, String field, String entry) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    field + " must be an integer in voter spec, got: " + entry + " (offending token: '" + s + "')", e);
        }
    }

    private static void runTopics(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: j-broker topics create|list|describe ...");
            return;
        }
        var sub = args[0];
        var rest = Arrays.copyOfRange(args, 1, args.length);
        String brokerAddr = flag(rest, "--broker", "127.0.0.1:9092");
        try (var client = clientFor(brokerAddr)) {
            switch (sub) {
                case "create" -> {
                    var topic = flag(rest, "--topic", null);
                    int partitions = Integer.parseInt(flag(rest, "--partitions", "1"));
                    int rf = Integer.parseInt(flag(rest, "--replication-factor", "1"));
                    client.createTopic(topic, partitions, rf);
                    System.out.println("created topic " + topic);
                }
                case "list" -> client.listTopics()
                        .forEach(t -> System.out.println(t.getTopic() + " (" + t.getPartitions() + " partitions, rf="
                                + t.getReplicationFactor() + ")"));
                case "describe" -> {
                    var topic = flag(rest, "--topic", null);
                    var t = client.describeTopic(topic);
                    System.out.println(t.getTopic() + ": partitions=" + t.getPartitions() + " rf="
                            + t.getReplicationFactor() + " created=" + t.getCreatedMillis());
                }
                default -> System.err.println("unknown subcommand: " + sub);
            }
        }
    }

    private static void runProduce(String[] args) throws Exception {
        String brokerAddr = flag(args, "--broker", "127.0.0.1:9092");
        String topic = flag(args, "--topic", null);
        int partition = Integer.parseInt(flag(args, "--partition", "0"));
        try (var client = clientFor(brokerAddr);
                var reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                long off = client.produce(topic, partition, line.getBytes(StandardCharsets.UTF_8));
                System.out.println("produced offset " + off);
            }
        }
    }

    private static void runConsumer(String[] args) throws Exception {
        String brokerAddr = flag(args, "--broker", "127.0.0.1:9092");
        String topic = flag(args, "--topic", null);
        int partition = Integer.parseInt(flag(args, "--partition", "0"));
        boolean fromBeginning = switchFlag(args, "--from-beginning");
        try (var client = clientFor(brokerAddr)) {
            long offset = fromBeginning ? 0L : -1L; // -1 not implemented yet — treat as 0 for now
            if (offset < 0) offset = 0L;
            while (true) {
                var batch = client.fetch(topic, partition, offset, 1024 * 1024);
                if (batch.isEmpty()) {
                    Thread.sleep(200);
                    continue;
                }
                for (var rec : batch) {
                    System.out.println(new String(rec, StandardCharsets.UTF_8));
                }
                offset += batch.size();
            }
        }
    }

    /**
     * Real consumer-group-aware CLI. Unlike {@code console-consumer}
     * (single-partition raw Fetch, no coordinator), this drives a full
     * {@link jbroker.broker.client.consumer.Consumer} session: join →
     * heartbeat → auto-assigned partitions → poll → commitSync on every
     * tick → clean leave on Ctrl-C. The Groups admin page populates while
     * this process runs.
     *
     * <p>Accepts repeated {@code --topic}.
     */
    private static void runGroupConsume(String[] args) throws Exception {
        String brokerAddr = flag(args, "--broker", "127.0.0.1:9092");
        String groupId = flag(args, "--group", null);
        if (groupId == null || groupId.isBlank()) {
            System.err.println("--group is required");
            System.exit(2);
        }
        var topics = new ArrayList<String>();
        for (int i = 0; i < args.length - 1; i++) {
            if ("--topic".equals(args[i])) topics.add(args[i + 1]);
        }
        if (topics.isEmpty()) {
            System.err.println("at least one --topic is required");
            System.exit(2);
        }
        var running = new java.util.concurrent.atomic.AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false), "j-broker-consume-shutdown"));
        consumeGroupLoop(brokerAddr, groupId, topics, running);
    }

    /**
     * Extracted for testability. Joins the group, polls until
     * {@code running} flips false, commits on every non-empty poll,
     * prints each record to stdout. Leaving is the consumer's close()
     * on try-with-resources; the caller flips {@code running} to stop.
     */
    static void consumeGroupLoop(
            String brokerAddr, String groupId, List<String> topics, java.util.concurrent.atomic.AtomicBoolean running)
            throws Exception {
        int colon = brokerAddr.indexOf(':');
        var host = brokerAddr.substring(0, colon);
        var port = Integer.parseInt(brokerAddr.substring(colon + 1));
        var cfg = jbroker.broker.client.consumer.ConsumerConfig.builder(groupId, host, port)
                .pollFetchDeadline(java.time.Duration.ofSeconds(5))
                .build();

        var keyDe = new jbroker.broker.client.consumer.StringDeserializer();
        var valueDe = new jbroker.broker.client.consumer.StringDeserializer();

        try (var consumer = new jbroker.broker.client.consumer.Consumer<>(cfg, keyDe, valueDe)) {
            consumer.subscribe(topics, jbroker.broker.client.consumer.RebalanceListener.NO_OP);
            System.err.println("j-broker consume: group=" + groupId + " subscribed=" + topics + " — Ctrl-C to leave");
            while (running.get()) {
                var batch = consumer.poll(java.time.Duration.ofMillis(500));
                if (batch.isEmpty()) continue;
                for (var rec : batch) {
                    System.out.println(rec.tp().getTopic() + "-" + rec.tp().getPartition() + "@" + rec.offset() + ": "
                            + rec.value());
                }
                try {
                    consumer.commitSync();
                } catch (Exception commitErr) {
                    System.err.println("commit failed (will retry on next tick): " + commitErr.getMessage());
                }
            }
        }
    }

    private static BrokerClient clientFor(String addr) {
        int colon = addr.indexOf(':');
        var host = addr.substring(0, colon);
        var port = Integer.parseInt(addr.substring(colon + 1));
        return new BrokerClient(host, port);
    }

    private BrokerApp() {}
}
