package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Milestone 6.7 end-to-end: exercise the idempotent-producer flow through the
 * public {@link BrokerClient} surface. Proves that a retry with the same
 * {@code (producer_id, epoch, base_sequence)} does not double-append.
 */
class IdempotentProduceEndToEndIT {

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void idempotentProduceDedupesRetriesInSingleBrokerCluster(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();
        var broker = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort));
        try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
            client.createTopic("orders", 1, 1);

            long producerId = client.initProducerId();
            assertThat(producerId).isGreaterThan(0L);

            // Three unique payloads at sequences 0, 1, 2. Each one is
            // retried once at the same sequence — the broker should dedup.
            for (int seq = 0; seq < 3; seq++) {
                byte[] payload = ("msg-" + seq).getBytes(StandardCharsets.UTF_8);
                long first = client.idempotentProduce("orders", 0, payload, producerId, 0, seq);
                long retry = client.idempotentProduce("orders", 0, payload, producerId, 0, seq);
                assertThat(retry).isEqualTo(first);
            }

            // Log end must be exactly 3 — no duplicated records.
            var records = client.fetchAll("orders", 0, 64 * 1024);
            assertThat(records).hasSize(3);
            for (int i = 0; i < 3; i++) {
                assertThat(new String(records.get(i), StandardCharsets.UTF_8)).isEqualTo("msg-" + i);
            }
        } finally {
            broker.close();
        }
    }
}
