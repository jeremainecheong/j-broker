package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import jbroker.app.testkit.BindRetry;
import jbroker.app.testkit.TestBrokerCluster;
import jbroker.broker.client.BrokerClient;
import jbroker.broker.controller.MembershipController;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Cluster-lifecycle gate: a spare broker joins a live 3-broker cluster as a
 * non-voting learner, catches up, and is promoted to voter — all while a
 * producer streams acks=all records. The join must not lose an acked record.
 *
 * <p>The newcomer boots knowing only the incumbent voters (itself absent), so
 * it never campaigns. {@code requestAddBroker} publishes its address (every
 * broker forms a Raft dial channel from the replicated registration) and then
 * drives the single-server membership change to completion.
 */
class AddBrokerLiveIT {

    // CI runners fsync + handshake 2–3× slower; scale the join budget so the
    // gate validates "the join completes", not laptop-specific timing.
    private static final int CI_MULT =
            "1".equals(System.getenv("JBROKER_CI")) || "true".equalsIgnoreCase(System.getenv("CI")) ? 4 : 1;

    private static final int RECORD_COUNT = 300;

    @Test
    void spareBrokerJoinsAsLearnerThenVoterUnderLoad(
            @TempDir Path d1, @TempDir Path d2, @TempDir Path d3, @TempDir Path spareBase) throws Exception {
        var dirs = new Path[] {d1, d2, d3};
        try (var cluster = TestBrokerCluster.start(
                3,
                2,
                (i, voters, ports) ->
                        new Broker.Config(new NodeId(i + 1), dirs[i], ports[i][0], ports[i][1], voters))) {
            var incumbents = List.of(cluster.broker(0), cluster.broker(1), cluster.broker(2));
            awaitSingleLeader(incumbents);
            awaitRegistryConvergence(incumbents, List.of(1, 2, 3));

            var leader = leaderOf(incumbents);
            try (var admin = new BrokerClient("127.0.0.1", leader.brokerPort())) {
                admin.createTopic("joined", 1, /*rf*/ 3);
            }

            // Background load: idempotent acks=all produces. Retrying the same
            // sequence dedupes at the broker, so a lost ack cannot duplicate a
            // record — the read-back below is exact. Every entry in `acked` is
            // durably replicated to all three ISR members before it is recorded.
            var acked = new CopyOnWriteArrayList<String>();
            var failure = new AtomicReference<Throwable>();
            var producer = new Thread(
                    () -> streamRecords(leader.brokerPort(), RECORD_COUNT, acked, failure), "join-load-producer");
            producer.start();

            // Let some records land so the join genuinely overlaps live traffic.
            awaitAtLeast(acked, 20, 10_000);

            // Boot the spare as a learner: voters are the incumbents, itself
            // absent, so it never campaigns; it catches up once named a learner.
            var incumbentVoters = incumbentVoterAddrs(cluster);
            var sparePorts = new int[2];
            var spare = BindRetry.startWithBindRetry(() -> {
                var p = BindRetry.freePorts(2);
                var dir = Files.createTempDirectory(spareBase, "spare-");
                var b = Broker.start(new Broker.Config(new NodeId(4), dir, p[0], p[1], incumbentVoters));
                sparePorts[0] = p[0];
                sparePorts[1] = p[1];
                return b;
            });
            try {
                boolean started = leader.requestAddBroker(new NodeId(4), "127.0.0.1", sparePorts[0], sparePorts[1]);
                assertThat(started).as("controller accepted the join").isTrue();

                awaitJoinComplete(leader, spare, 30_000L * CI_MULT);

                producer.join(30_000L * CI_MULT);
                assertThat(producer.isAlive())
                        .as("producer finished all records")
                        .isFalse();
                assertThat(failure.get()).as("producer saw no fatal error").isNull();
                assertThat(acked).as("all records acked").hasSize(RECORD_COUNT);

                // Zero loss: every acked record is readable, in order, no gaps.
                try (var reader = new BrokerClient("127.0.0.1", leader.brokerPort())) {
                    var read = reader.fetchAll("joined", 0, 1 << 20).stream()
                            .map(b -> new String(b, StandardCharsets.UTF_8))
                            .toList();
                    assertThat(read).containsExactlyElementsOf(acked);
                }

                assertThat(leader.membershipProgress().phase()).isEqualTo(MembershipController.Phase.DONE);
                // The newcomer, now a voter, has replicated the metadata log:
                // it knows every broker and the topic created before it joined.
                assertThat(spare.brokerRegistry().knownBrokerIds()).contains(1, 2, 3, 4);
                assertThat(spare.topics().list().stream().map(t -> t.topic())).contains("joined");
            } finally {
                spare.close();
            }
        }
    }

    /** Produce {@code count} idempotent acks=all records; retry each until it lands. */
    private static void streamRecords(
            int brokerPort, int count, List<String> acked, AtomicReference<Throwable> failure) {
        try (var client = new BrokerClient("127.0.0.1", brokerPort)) {
            long producerId = client.initProducerId();
            for (int seq = 0; seq < count; seq++) {
                var value = "rec-" + seq;
                long attemptDeadline = System.currentTimeMillis() + 30_000L * CI_MULT;
                while (true) {
                    try {
                        client.idempotentProduceAcksAll(
                                "joined", 0, value.getBytes(StandardCharsets.UTF_8), producerId, 0, seq);
                        acked.add(value);
                        break;
                    } catch (RuntimeException retriable) {
                        // Membership churn / a briefly-lagging ISR can surface a
                        // transient error; the same sequence dedupes on retry.
                        if (System.currentTimeMillis() > attemptDeadline) throw retriable;
                        sleepQuietly(20);
                    }
                }
            }
        } catch (Throwable t) {
            failure.set(t);
        }
    }

    private static List<VoterAddress> incumbentVoterAddrs(TestBrokerCluster cluster) {
        return List.of(
                new VoterAddress(new NodeId(1), "127.0.0.1", cluster.raftPort(0), cluster.brokerPort(0)),
                new VoterAddress(new NodeId(2), "127.0.0.1", cluster.raftPort(1), cluster.brokerPort(1)),
                new VoterAddress(new NodeId(3), "127.0.0.1", cluster.raftPort(2), cluster.brokerPort(2)));
    }

    private static void awaitJoinComplete(Broker leader, Broker spare, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean promoted = leader.membershipProgress().phase() == MembershipController.Phase.DONE;
            boolean caughtUp = spare.brokerRegistry().knownBrokerIds().containsAll(List.of(1, 2, 3, 4));
            if (promoted && caughtUp) return;
            Thread.sleep(100);
        }
        throw new AssertionError(
                "broker did not join (phase=" + leader.membershipProgress().phase() + ", spare knows="
                        + spare.brokerRegistry().knownBrokerIds() + ") within " + timeoutMs + "ms");
    }

    private static void awaitAtLeast(List<String> list, int n, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (list.size() >= n) return;
            Thread.sleep(50);
        }
        throw new AssertionError("only " + list.size() + " records produced within " + timeoutMs + "ms");
    }

    private static void awaitSingleLeader(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (brokers.stream().filter(b -> b.role() == Role.LEADER).count() == 1) return;
            Thread.sleep(50);
        }
        throw new AssertionError("no single leader within 10s");
    }

    private static void awaitRegistryConvergence(List<Broker> brokers, List<Integer> expected)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (brokers.stream()
                    .allMatch(b -> b.brokerRegistry().knownBrokerIds().containsAll(expected))) return;
            Thread.sleep(50);
        }
        throw new AssertionError("broker registry did not converge within 5s");
    }

    private static Broker leaderOf(List<Broker> brokers) {
        return brokers.stream().filter(b -> b.role() == Role.LEADER).findFirst().orElseThrow();
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
