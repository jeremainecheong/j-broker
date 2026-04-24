package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
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
 * CI-grade 10k-client hot-path smoke. Mirrors
 * {@link TenThousandClientsIT}'s goal (prove the broker's
 * acceptor + virtual-thread handler pool cope with high connection
 * churn) but trades 10k-concurrent for 250-concurrent × 40 rounds so it
 * runs under a standard {@code ulimit -n 1024}. The sibling @slow test
 * still covers the steady-state "all 10k sockets open simultaneously"
 * scenario; operators opt into that run explicitly.
 *
 * <p>hardening pass widens the workload: instead of every client calling
 * {@code listTopics()}, the 10k clients split 40/40/20 across
 * produce / fetch / listTopics so the test actually exercises the
 * hot paths (ProduceHandler partition locks, FetchHandler session
 * cache, admin path, virtual-thread scheduler) rather than the
 * listTopics short-circuit alone. After all rounds we drain every
 * partition and assert the total record count matches the number of
 * successful produces — a cheap but tight end-to-end invariant that
 * would catch lost produces, duplicated writes, or torn fetches.
 */
class TenThousandClientsCiGradeIT {

    private static final int CLIENTS_PER_ROUND = 250;
    private static final int ROUNDS = 40;
    private static final int TOTAL_CONNECTIONS = CLIENTS_PER_ROUND * ROUNDS;
    private static final int PARTITIONS = 3;
    private static final int FETCH_MAX_BYTES = 16 * 1024;

    @Test
    void tenThousandConnectionsMixedWorkloadAllSucceed(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
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
                client.createTopic("ci-scale", PARTITIONS, 3);
                long deadline = System.currentTimeMillis() + 5_000;
                while (!(br1.topics().exists("ci-scale")
                                && br2.topics().exists("ci-scale")
                                && br3.topics().exists("ci-scale"))
                        && System.currentTimeMillis() < deadline) {
                    Thread.sleep(25);
                }
            }

            var produceOk = new AtomicInteger(0);
            var produceFail = new AtomicInteger(0);
            var fetchOk = new AtomicInteger(0);
            var fetchFail = new AtomicInteger(0);
            var listOk = new AtomicInteger(0);
            var listFail = new AtomicInteger(0);
            long t0 = System.nanoTime();

            try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int round = 0; round < ROUNDS; round++) {
                    var tasks = new java.util.ArrayList<Callable<Void>>(CLIENTS_PER_ROUND);
                    for (int i = 0; i < CLIENTS_PER_ROUND; i++) {
                        int globalIdx = round * CLIENTS_PER_ROUND + i;
                        int op = globalIdx % 5; // 0,1=produce  2,3=fetch  4=list
                        int partition = globalIdx % PARTITIONS;
                        tasks.add(() -> {
                            try (var c = new BrokerClient("127.0.0.1", leaderPort)) {
                                if (op == 0 || op == 1) {
                                    byte[] v = ("v-" + globalIdx).getBytes(StandardCharsets.UTF_8);
                                    c.produce("ci-scale", partition, v);
                                    produceOk.incrementAndGet();
                                } else if (op == 2 || op == 3) {
                                    c.fetch("ci-scale", partition, 0L, FETCH_MAX_BYTES);
                                    fetchOk.incrementAndGet();
                                } else {
                                    var topics = c.listTopics();
                                    if (topics.stream().anyMatch(t -> "ci-scale".equals(t.getTopic()))) {
                                        listOk.incrementAndGet();
                                    } else {
                                        listFail.incrementAndGet();
                                    }
                                }
                            } catch (Exception e) {
                                if (op == 0 || op == 1) produceFail.incrementAndGet();
                                else if (op == 2 || op == 3) fetchFail.incrementAndGet();
                                else listFail.incrementAndGet();
                            }
                            return null;
                        });
                    }
                    for (var f : exec.invokeAll(tasks)) f.get();
                }
            }
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            int totalOk = produceOk.get() + fetchOk.get() + listOk.get();
            int totalFail = produceFail.get() + fetchFail.get() + listFail.get();
            System.out.printf(
                    "%d mixed ops in %dms — produce(ok=%d fail=%d) fetch(ok=%d fail=%d) list(ok=%d fail=%d)%n",
                    TOTAL_CONNECTIONS,
                    elapsedMs,
                    produceOk.get(),
                    produceFail.get(),
                    fetchOk.get(),
                    fetchFail.get(),
                    listOk.get(),
                    listFail.get());

            // Allow a small rate of transient failures from client-side
            // ephemeral-port reuse timing on macOS. The test is about the
            // broker's steady-state throughput, not kernel networking
            // edge cases.
            assertThat(totalOk)
                    .as("≥99.5%% of the 10k mixed ops should succeed (ok=%d fail=%d)", totalOk, totalFail)
                    .isGreaterThanOrEqualTo((int) (TOTAL_CONNECTIONS * 0.995));

            // End-to-end invariant: every record the leader accepted must be
            // readable via a subsequent fetch. Drain every partition and
            // compare against produceOk — this catches lost writes, duplicate
            // persistence, and torn fetches all in one cheap assertion.
            try (var client = new BrokerClient("127.0.0.1", leaderPort)) {
                int drained = 0;
                for (int p = 0; p < PARTITIONS; p++) {
                    drained += client.fetchAll("ci-scale", p, FETCH_MAX_BYTES).size();
                }
                assertThat(drained)
                        .as("fetched record count must equal successful produces")
                        .isEqualTo(produceOk.get());
            }
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
