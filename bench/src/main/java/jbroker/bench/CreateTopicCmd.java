package jbroker.bench;

import java.util.LinkedHashMap;
import java.util.Locale;
import jbroker.broker.client.BrokerClient;

/**
 * Topic bootstrap for bench scripts. The admin CLI's {@code topics
 * create} takes no config map, and the flush-policy comparison needs
 * per-topic overrides like {@code flush.messages=1} — so the harness
 * carries its own creator. An already-existing topic is treated as
 * success (rerun case); any other failure exits non-zero.
 *
 * <pre>
 *   j-broker-bench create-topic --broker HOST:PORT --topic T
 *                               [--partitions N] [--rf N]
 *                               [--config key=value]...
 *                               [--tls-trust CA --tls-cert C --tls-key K]
 * </pre>
 */
public final class CreateTopicCmd {

    private CreateTopicCmd() {}

    static void run(String[] args) {
        String broker = BenchArgs.get(args, "--broker", "127.0.0.1:9092");
        String topic = BenchArgs.get(args, "--topic", null);
        int partitions = BenchArgs.getInt(args, "--partitions", 1);
        int rf = BenchArgs.getInt(args, "--rf", 1);
        var tls = BenchArgs.tlsFromArgs(args);

        if (topic == null) {
            System.err.println("--topic required");
            System.exit(2);
            return;
        }
        var config = new LinkedHashMap<String, String>();
        for (var kv : BenchArgs.getAll(args, "--config")) {
            int eq = kv.indexOf('=');
            if (eq <= 0) {
                System.err.println("--config expects key=value, got: " + kv);
                System.exit(2);
                return;
            }
            config.put(kv.substring(0, eq).trim(), kv.substring(eq + 1).trim());
        }

        int colon = broker.indexOf(':');
        try (var client =
                new BrokerClient(broker.substring(0, colon), Integer.parseInt(broker.substring(colon + 1)), tls)) {
            try {
                client.createTopicWithConfig(topic, partitions, rf, config);
                System.out.println("created topic " + topic);
            } catch (RuntimeException e) {
                var msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
                if (msg.contains("exist")) {
                    System.out.println("topic " + topic + " already exists — reusing");
                    return;
                }
                System.err.println("create-topic failed: " + e.getMessage());
                System.exit(1);
            }
        }
    }
}
