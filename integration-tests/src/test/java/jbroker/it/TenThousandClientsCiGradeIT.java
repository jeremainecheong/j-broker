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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * CI-grade 10k-client connection smoke. Mirrors
 * {@link TenThousandClientsIT}'s goal (prove the broker's
 * acceptor + virtual-thread handler pool cope with high connection
 * churn) but trades 10k-concurrent for 250-concurrent × 40 rounds so it
 * runs under a standard {@code ulimit -n 1024}. The sibling @slow test
 * still covers the steady-state "all 10k sockets open simultaneously"
 * scenario; operators opt into that run explicitly.
 *
 * <p>Running under default limits means this IT earns a spot in CI's
 * green-run coverage, closing the v1.1 gap where high-client-count
 * behavior was tested only on a developer laptop with a bumped ulimit.
 */
class TenThousandClientsCiGradeIT {

    private static final int CLIENTS_PER_ROUND = 250;
    private static final int ROUNDS = 40;
    private static final int TOTAL_CONNECTIONS = CLIENTS_PER_ROUND * ROUNDS;

    @Test
    void tenThousandConnectionsAcrossSerialisedRoundsAllSucceed(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
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

            try (var client = new BrokerClient("127.0.0.1", leaderPort)) {
                client.createTopic("ci-scale", 3, 3);
                long deadline = System.currentTimeMillis() + 5_000;
                while (!(br1.topics().exists("ci-scale")
                                && br2.topics().exists("ci-scale")
                                && br3.topics().exists("ci-scale"))
                        && System.currentTimeMillis() < deadline) {
                    Thread.sleep(25);
                }
            }

            var successes = new AtomicInteger(0);
            var failures = new AtomicInteger(0);
            long t0 = System.nanoTime();

            try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int round = 0; round < ROUNDS; round++) {
                    var tasks = new java.util.ArrayList<Callable<Void>>(CLIENTS_PER_ROUND);
                    for (int i = 0; i < CLIENTS_PER_ROUND; i++) {
                        tasks.add(() -> {
                            try (var c = new BrokerClient("127.0.0.1", leaderPort)) {
                                var topics = c.listTopics();
                                if (topics.stream().anyMatch(t -> "ci-scale".equals(t.getTopic()))) {
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
                    for (var f : exec.invokeAll(tasks)) f.get();
                }
            }
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            System.out.printf(
                    "%d connections (%d clients x %d rounds) in %dms (success=%d fail=%d)%n",
                    TOTAL_CONNECTIONS, CLIENTS_PER_ROUND, ROUNDS, elapsedMs, successes.get(), failures.get());

            // Allow a small rate of transient failures from client-side
            // ephemeral-port reuse timing on macOS. The test is about the
            // broker's steady-state throughput, not kernel networking
            // edge cases.
            assertThat(successes.get())
                    .as("≥99.5% of the 10k connections should complete their listTopics round-trip")
                    .isGreaterThanOrEqualTo((int) (TOTAL_CONNECTIONS * 0.995));
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
