package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import jbroker.app.Broker;
import jbroker.app.VoterAddress;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * E2E-10-1 at full scale — 10k concurrent client connections each
 * issuing one operation. Stresses the broker's acceptor path + virtual-
 * thread handler pool.
 *
 * <p>Tagged {@code slow}: default CI skips; opt into with
 * {@code JBROKER_RUN_SLOW_TESTS=1 ./gradlew test}. Requires a high
 * open-file-descriptor ulimit (the smoke run uses ~10k ephemeral sockets
 * at peak); on macOS a {@code ulimit -n 20000} beforehand is usually
 * necessary.
 */
@Tag("slow")
class E2E_10_1_TenThousandClientsIT {

    private static final int CLIENTS = 10_000;

    @Test
    void tenThousandConcurrentClientsAllSucceed(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3) throws Exception {
        int r1 = freePort(), r2 = freePort(), r3 = freePort();
        int b1 = freePort(), b2 = freePort(), b3 = freePort();
        var voters = List.of(
                new VoterAddress(new NodeId(1), "127.0.0.1", r1, b1),
                new VoterAddress(new NodeId(2), "127.0.0.1", r2, b2),
                new VoterAddress(new NodeId(3), "127.0.0.1", r3, b3));

        try (var br1 = Broker.start(new Broker.Config(new NodeId(1), d1, r1, b1, voters));
                var br2 = Broker.start(new Broker.Config(new NodeId(2), d2, r2, b2, voters));
                var br3 = Broker.start(new Broker.Config(new NodeId(3), d3, r3, b3, voters))) {
            waitForClusterReady(br1, br2, br3);
            int leaderPort = br1.role() == Role.LEADER ? b1 : br2.role() == Role.LEADER ? b2 : b3;

            // Seed a topic so each client has something to list / describe.
            try (var client = new BrokerClient("127.0.0.1", leaderPort)) {
                client.createTopic("scale-10k", 3, 3);
                long deadline = System.currentTimeMillis() + 5_000;
                while (!(br1.topics().exists("scale-10k")
                                && br2.topics().exists("scale-10k")
                                && br3.topics().exists("scale-10k"))
                        && System.currentTimeMillis() < deadline) {
                    Thread.sleep(25);
                }
            }

            // Spawn CLIENTS virtual threads; each opens its own BrokerClient
            // (so each connection is a distinct TCP socket), issues one
            // listTopics, and closes. Track success count + failure count.
            var successes = new AtomicInteger(0);
            var failures = new AtomicInteger(0);
            try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
                var tasks = new java.util.ArrayList<Callable<Void>>(CLIENTS);
                for (int i = 0; i < CLIENTS; i++) {
                    tasks.add(() -> {
                        try (var c = new BrokerClient("127.0.0.1", leaderPort)) {
                            var topics = c.listTopics();
                            if (topics.stream().anyMatch(t -> "scale-10k".equals(t.getTopic()))) {
                                successes.incrementAndGet();
                            } else {
                                failures.incrementAndGet();
                            }
                        } catch (Exception e) {
                            failures.incrementAndGet();
                        }
                        return null;
                    });
                }
                long t0 = System.nanoTime();
                for (var f : exec.invokeAll(tasks)) f.get();
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
                System.out.printf(
                        "E2E-10-1: %d clients complete in %dms (success=%d fail=%d)%n",
                        CLIENTS, elapsedMs, successes.get(), failures.get());
            }

            // Accept a small rate of TCP setup failures (kernel accept
            // backlog, ephemeral port exhaustion on the client side). The
            // test is about the broker's handler throughput, not the
            // laptop's networking stack.
            assertThat(successes.get())
                    .as("≥99.5% of clients should complete their listTopics round-trip")
                    .isGreaterThanOrEqualTo((int) (CLIENTS * 0.995));
        }
    }

    private static void waitForClusterReady(Broker b1, Broker b2, Broker b3) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            int leaders = (b1.role() == Role.LEADER ? 1 : 0)
                    + (b2.role() == Role.LEADER ? 1 : 0)
                    + (b3.role() == Role.LEADER ? 1 : 0);
            boolean full = b1.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3))
                    && b2.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3))
                    && b3.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3));
            if (leaders == 1 && full) return;
            Thread.sleep(50);
        }
        throw new IllegalStateException("cluster did not converge in 15s");
    }

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
