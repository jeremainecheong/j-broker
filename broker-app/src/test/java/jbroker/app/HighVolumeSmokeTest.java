package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.BindException;
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

    @Test
    void hundredThousandMessagesRoundTripInOrder(@TempDir Path dir) throws Exception {
        final int total = 100_000;

        // Broker.start may fail with a bind error if another process has
        // grabbed our chosen port between freePort()'s socket close and
        // Broker's own bind — the classic close-then-bind TOCTOU race.
        // Retry with fresh ports; 5 attempts makes the flake probability
        // (p_fail ≈ 1e-3 on laptop, up to 1e-2 on shared CI) ^5 negligible.
        var broker = startBrokerWithBindRetry(dir);
        try (var client = new BrokerClient("127.0.0.1", broker.brokerPort())) {
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

            System.out.printf("100k round-trip: produce=%.2fs fetch=%.2fs%n", produceNs / 1e9, fetchNs / 1e9);

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

    private static Broker startBrokerWithBindRetry(Path dir) throws Exception {
        Exception lastFailure = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            int raftPort = freePort();
            int brokerPort = freePort();
            try {
                return Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort));
            } catch (Exception e) {
                if (!isBindRace(e)) throw e;
                lastFailure = e;
            }
        }
        throw new AssertionError(
                "Broker.start hit a bind race 5× in a row — real networking issue, not a flake", lastFailure);
    }

    /**
     * True for bind-time failures that can be retried with a fresh port:
     * {@link BindException}, Netty's {@code NativeIoException} from a
     * failed bind, or anything in the cause chain whose message says
     * "Address already in use" / "bind".
     */
    private static boolean isBindRace(Throwable t) {
        while (t != null) {
            if (t instanceof BindException) return true;
            if (t.getClass().getName().contains("NativeIoException")) {
                String msg = t.getMessage();
                if (msg != null && (msg.contains("Address already in use") || msg.contains("bind"))) return true;
            }
            t = t.getCause();
        }
        return false;
    }

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
