package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import jbroker.app.Broker;
import jbroker.app.VoterAddress;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Audit-finding #3 — asymmetric partition primitive + split-brain
 * avoidance. Before this, {@code ChaosState.blockPeer} was symmetric:
 * both outbound and inbound RPCs between a pair of brokers were blocked
 * together. That symmetric shape doesn't exercise the Raft failure mode
 * that actually matters: a leader hearing heartbeats from followers
 * while unable to broadcast its own (one-way outbound partition), or
 * vice versa.
 *
 * <p>This test creates an asymmetric partition where a minority broker
 * (the current Raft leader) has its outbound traffic to both peers
 * blocked — it can still receive inbound RPCs, but can't broadcast
 * AppendEntries. The invariant is: Raft must prevent a dual-leader
 * scenario. The majority of two peers must elect a new leader at a
 * higher term, and the isolated ex-leader must step down (or simply
 * fail to commit anything) once it detects a higher term.
 */
class AsymmetricPartitionIT {

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void oneWayOutboundPartitionOfLeaderDoesNotCauseDualLeader(@TempDir Path d1, @TempDir Path d2, @TempDir Path d3)
            throws Exception {
        int r1 = freePort(), r2 = freePort(), r3 = freePort();
        int b1 = freePort(), b2 = freePort(), b3 = freePort();
        int c1 = freePort(), c2 = freePort(), c3 = freePort();
        var voters = List.of(
                new VoterAddress(new NodeId(1), "127.0.0.1", r1, b1),
                new VoterAddress(new NodeId(2), "127.0.0.1", r2, b2),
                new VoterAddress(new NodeId(3), "127.0.0.1", r3, b3));

        var br1 = Broker.start(new Broker.Config(new NodeId(1), d1, r1, b1, voters).withChaosPort(c1));
        var br2 = Broker.start(new Broker.Config(new NodeId(2), d2, r2, b2, voters).withChaosPort(c2));
        var br3 = Broker.start(new Broker.Config(new NodeId(3), d3, r3, b3, voters).withChaosPort(c3));
        var brokers = new ArrayList<>(List.of(br1, br2, br3));

        try {
            awaitSingleRaftLeader(brokers);

            // Identify the current leader. We'll asymmetrically partition it
            // so it can still *receive* RPCs (e.g., AppendEntries from a new
            // leader at higher term), but can't send any outbound traffic.
            var isolatedLeader = brokers.stream()
                    .filter(b -> b.role() == Role.LEADER)
                    .findFirst()
                    .orElseThrow();
            int isolatedId = brokerIdOf(isolatedLeader);
            long isolatedTerm = isolatedLeader.role() == Role.LEADER
                    ? // capture pre-partition term for comparison
                    getCurrentTerm(isolatedLeader)
                    : 0L;
            var survivors = brokers.stream().filter(b -> b != isolatedLeader).toList();

            // Install outbound-only blocks on the isolated leader, pointing at
            // both peers. Its view of the cluster degrades: it can still
            // *read* heartbeats + vote requests but its replies never land.
            for (var peer : survivors) {
                int peerId = brokerIdOf(peer);
                isolatedLeader.brokerRegistry(); // touch registry to keep JIT from optimising away accesses
                directlyBlockOutbound(isolatedLeader, peerId);
            }

            // Majority quorum (the two survivors) must detect missing
            // AppendEntries from the isolated leader and elect a new leader
            // at term > isolatedTerm. Budget: 10s on local, 20s on CI, since
            // pre-vote + election timeout + commit can stack up.
            long budget = ("1".equals(System.getenv("JBROKER_CI")) || "true".equalsIgnoreCase(System.getenv("CI")))
                    ? 20_000
                    : 10_000;
            long deadline = System.currentTimeMillis() + budget;
            Broker newLeader = null;
            while (System.currentTimeMillis() < deadline) {
                for (var s : survivors) {
                    if (s.role() == Role.LEADER) {
                        newLeader = s;
                        break;
                    }
                }
                if (newLeader != null) break;
                Thread.sleep(100);
            }

            assertThat(newLeader)
                    .as(
                            "a surviving broker must take over Raft leadership within %dms of an outbound "
                                    + "partition of the previous leader — otherwise Raft can't make progress in a "
                                    + "3-node cluster with 2 healthy peers"
                                    + System.lineSeparator() + "isolatedTerm=" + isolatedTerm
                                    + ", isolatedId=" + isolatedId,
                            budget)
                    .isNotNull();

            long newLeaderTerm = getCurrentTerm(newLeader);
            assertThat(newLeaderTerm)
                    .as("new leader's term must be strictly greater than the isolated leader's")
                    .isGreaterThan(isolatedTerm);

            // Invariant: the isolated ex-leader must NOT still believe it is
            // leader at the NEW term. (It may still locally return Role.LEADER
            // for its old term, but the new higher term must force it to step
            // down if it receives inbound AppendEntries — which it can, since
            // only outbound is blocked.) Give it a moment to observe the new
            // term via inbound traffic.
            long observeDeadline = System.currentTimeMillis() + 3_000;
            while (System.currentTimeMillis() < observeDeadline) {
                if (isolatedLeader.role() != Role.LEADER) break;
                Thread.sleep(50);
            }
            assertThat(isolatedLeader.role())
                    .as("isolated ex-leader must step down once it observes the new higher term")
                    .isNotEqualTo(Role.LEADER);
        } finally {
            for (var b : brokers) {
                try {
                    b.close();
                } catch (Exception ignored) {
                    // best-effort shutdown
                }
            }
        }
    }

    // ---------------- reflection helpers ----------------

    /**
     * Reach into the broker to flip the outbound-only block. Production code
     * calls the chaos HTTP endpoint; tests bypass the JSON dance to keep
     * scenarios readable.
     */
    private static void directlyBlockOutbound(Broker broker, int peerId) throws Exception {
        var state = chaosState(broker);
        state.blockOutboundToPeer(peerId);
    }

    private static jbroker.broker.chaos.ChaosState chaosState(Broker broker) throws Exception {
        // Broker.chaosHttp holds the ChaosState; reach through it via reflection.
        var field = Broker.class.getDeclaredField("chaosHttp");
        field.setAccessible(true);
        var http = (jbroker.broker.chaos.ChaosHttpServer) field.get(broker);
        var stateField = http.getClass().getDeclaredField("state");
        stateField.setAccessible(true);
        return (jbroker.broker.chaos.ChaosState) stateField.get(http);
    }

    private static int brokerIdOf(Broker broker) throws Exception {
        var f = Broker.class.getDeclaredField("raftDriver");
        f.setAccessible(true);
        var rd = f.get(broker);
        var sidField = rd.getClass().getDeclaredField("selfId");
        sidField.setAccessible(true);
        return ((NodeId) sidField.get(rd)).value();
    }

    private static long getCurrentTerm(Broker broker) throws Exception {
        var f = Broker.class.getDeclaredField("raftDriver");
        f.setAccessible(true);
        var rd = f.get(broker);
        var termMethod = rd.getClass().getMethod("currentTerm");
        var term = termMethod.invoke(rd);
        var valueMethod = term.getClass().getMethod("value");
        return (long) valueMethod.invoke(term);
    }

    private static void awaitSingleRaftLeader(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            long leaders = brokers.stream().filter(b -> b.role() == Role.LEADER).count();
            if (leaders == 1) return;
            Thread.sleep(50);
        }
        throw new AssertionError("no single Raft leader within 10s");
    }
}
