package jbroker.app;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;

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
            case "admin" -> AdminCli.run(Arrays.copyOfRange(args, 1, args.length));
            default -> usage();
        }
    }

    private static void usage() {
        System.err.println("Usage: j-broker server|topics|produce|console-consumer|admin ...");
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
        int chaosPort = Integer.parseInt(flag(args, "--chaos-port", "-1"));
        int consumerOffsetsPartitions = Integer.parseInt(flag(
                args,
                "--consumer-offsets-partitions",
                String.valueOf(jbroker.broker.ConsumerOffsetsTopic.PARTITION_COUNT)));

        List<VoterAddress> voters;
        if (votersSpec == null || votersSpec.isBlank()) {
            // Single-broker dev server: self is the sole voter. Preserves the
            // pre-v1.1 default so existing ./gradlew :broker-app:run flows
            // keep working.
            voters = List.of(new VoterAddress(new NodeId(id), "127.0.0.1", raftPort, brokerPort));
        } else {
            voters = parseVoters(votersSpec);
            if (voters.stream().noneMatch(v -> v.id().value() == id)) {
                throw new IllegalArgumentException(
                        "--voters must include self (id=" + id + "), got: " + votersSpec);
            }
        }

        var config = new Broker.Config(
                        new NodeId(id), Path.of(dataDir), raftPort, brokerPort, voters, consumerOffsetsPartitions)
                .withChaosPort(chaosPort);
        var broker = Broker.start(config);
        Runtime.getRuntime().addShutdownHook(new Thread(broker::close, "broker-shutdown"));
        System.out.println("j-broker listening on " + brokerPort
                + " (id=" + id + ", raft=" + raftPort + ", data=" + dataDir
                + ", voters=" + voters.size() + (chaosPort >= 0 ? ", chaos=" + chaosPort : "")
                + ")");
        Thread.currentThread().join();
    }

    /**
     * Parses a voter list like {@code 1@broker1:9192:9092,2@broker2:9192:9092}.
     * {@code raftPort} and {@code brokerPort} are the ports each voter binds
     * for Raft peer RPC and broker gRPC respectively — the host:port pair the
     * rest of the cluster uses to dial them.
     */
    static List<VoterAddress> parseVoters(String spec) {
        var out = new ArrayList<VoterAddress>();
        for (var entry : spec.split(",")) {
            var trimmed = entry.trim();
            if (trimmed.isEmpty()) continue;
            int at = trimmed.indexOf('@');
            if (at <= 0 || at == trimmed.length() - 1) {
                throw new IllegalArgumentException("voter spec must be ID@HOST:RAFT:BROKER, got: " + trimmed);
            }
            int vid = Integer.parseInt(trimmed.substring(0, at));
            var hostPorts = trimmed.substring(at + 1).split(":");
            if (hostPorts.length != 3) {
                throw new IllegalArgumentException("voter spec must be ID@HOST:RAFT:BROKER, got: " + trimmed);
            }
            out.add(new VoterAddress(
                    new NodeId(vid),
                    hostPorts[0],
                    Integer.parseInt(hostPorts[1]),
                    Integer.parseInt(hostPorts[2])));
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("--voters must contain at least one entry");
        }
        return List.copyOf(out);
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
            long offset = fromBeginning ? 0L : -1L; // -1 not implemented yet — treat as 0 for Phase 5
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

    private static BrokerClient clientFor(String addr) {
        int colon = addr.indexOf(':');
        var host = addr.substring(0, colon);
        var port = Integer.parseInt(addr.substring(colon + 1));
        return new BrokerClient(host, port);
    }

    private BrokerApp() {}
}
