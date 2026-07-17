package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import jbroker.broker.ConsumerOffsetsTopic;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A broker booted from {@code j-broker.yaml} actually enforces what the
 * file says (R7.1) — the layering test's missing half. The file sets a
 * 2 KiB cluster {@code max.message.bytes}; a produce over that limit must
 * be refused over the wire with MESSAGE_TOO_LARGE, which also pins the
 * cluster-default plumbing deferred from the per-topic max-message work.
 */
class ConfigFileBootIT {

    @Test
    void brokerBootedFromYamlEnforcesItsClusterDefaults(@TempDir Path dir) throws Exception {
        var yaml = dir.resolve("j-broker.yaml");
        Files.writeString(
                yaml,
                """
                data.dir: %s
                max.message.bytes: 2048
                """
                        .formatted(dir.resolve("data")));

        // Ports come from the testkit's bind-retry allocation, layered
        // over the file as flags — exactly the override path an operator
        // would use.
        var node = jbroker.app.testkit.TestBrokers.start((rp, bp) -> {
            try {
                var config = ServerConfig.resolve(yaml, Map.of(), new String[] {
                    "--raft-port", String.valueOf(rp), "--broker-port", String.valueOf(bp)
                });
                assertThat(config.validate()).isEmpty();
                return BrokerApp.toBrokerConfig(config);
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        });
        try (var client = new jbroker.broker.client.BrokerClient("127.0.0.1", node.brokerPort())) {
            waitReady(node.broker());
            client.createTopic("bounded", 1, 1);

            client.produce("bounded", 0, new byte[512]);
            assertThatThrownBy(() -> client.produce("bounded", 0, new byte[4096]))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("max.message.bytes");
            assertThat(client.fetchAll("bounded", 0, 1 << 20)).hasSize(1);
        } finally {
            node.broker().close();
        }
    }

    private static void waitReady(Broker broker) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (broker.topics().describe(ConsumerOffsetsTopic.NAME).isPresent()
                    && broker.brokerRegistry().addressFor(1).isPresent()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("broker not ready within 10s");
    }
}
