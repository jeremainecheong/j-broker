package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * High-volume produce/consume: PRD §Phase-5 DoD says "producing 100k messages
 * and consuming them returns the same 100k in order within partitions". This
 * test exercises exactly that against a live in-process broker.
 *
 * <p>Tagged by filename, not scope — we still want it to run on CI because
 * it's the truest end-to-end coverage we have. Each message is small so the
 * whole run fits easily inside the CI budget.
 */
class HighVolumeSmokeTest {

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void hundredThousandMessagesRoundTripInOrder(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();
        final int total = 100_000;

        var broker = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort));
        try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
            client.createTopic("bulk", 1, 1);

            long t0 = System.nanoTime();
            for (int i = 0; i < total; i++) {
                client.produce("bulk", 0, Integer.toString(i).getBytes(StandardCharsets.UTF_8));
            }
            long produceNs = System.nanoTime() - t0;

            long t1 = System.nanoTime();
            var fetched = new ArrayList<String>(total);
            long offset = 0;
            while (fetched.size() < total) {
                var batch = client.fetch("bulk", 0, offset, 1_048_576);
                if (batch.isEmpty()) break;
                for (var v : batch) fetched.add(new String(v, StandardCharsets.UTF_8));
                offset += batch.size();
            }
            long fetchNs = System.nanoTime() - t1;

            System.out.printf(
                    "100k round-trip: produce=%.2fs fetch=%.2fs%n", produceNs / 1e9, fetchNs / 1e9);

            assertThat(fetched).hasSize(total);
            // Strict ordering: the i-th fetched value must be the string "i".
            for (int i = 0; i < total; i++) {
                if (!fetched.get(i).equals(Integer.toString(i))) {
                    throw new AssertionError("out of order at i=" + i + ": " + fetched.get(i));
                }
            }
        } finally {
            broker.close();
        }
    }
}
