package jbroker.broker.replication;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import jbroker.broker.BrokerLiveness;
import jbroker.broker.TopicManager;
import jbroker.proto.raft.MetadataRecord;
import jbroker.proto.raft.PartitionChangeRecord;
import jbroker.raft.core.Role;
import org.junit.jupiter.api.Test;

class BrokerFencerTest {

    /** Captures each propose for assertion. */
    private static final class CapturingProposer implements BrokerFencer.MetadataProposer {
        final List<PartitionChangeRecord> proposals = new ArrayList<>();

        @Override
        public void propose(byte[] payload) {
            try {
                var r = MetadataRecord.parseFrom(payload);
                if (r.hasPartitionChange()) proposals.add(r.getPartitionChange());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static long staleThresholdNanos() {
        return TimeUnit.SECONDS.toNanos(3);
    }

    @Test
    void staleLeaderTriggersFailoverWithBumpedEpoch() {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 3, 0L);
        tm.onPartitionChange("t", 0, /*leader*/ 2, List.of(2, 1, 3), List.of(2, 1, 3), /*leaderEpoch*/ 5, 0);

        var liveness = new BrokerLiveness();
        // now = 10s; broker 2's last signal was at 0s → stale by 10s
        liveness.recordSignal(1, 0L, 10_000_000_000L);
        liveness.recordSignal(2, 0L, 0L); // stale
        liveness.recordSignal(3, 0L, 10_000_000_000L);

        var proposer = new CapturingProposer();
        var fencer =
                new BrokerFencer(/*selfBrokerId*/ 1, tm, liveness, proposer, () -> Role.LEADER, staleThresholdNanos());

        fencer.tick(/*nowNanos*/ 10_000_000_000L);

        assertThat(proposer.proposals).hasSize(1);
        var p = proposer.proposals.get(0);
        assertThat(p.getTopic()).isEqualTo("t");
        assertThat(p.getPartition()).isEqualTo(0);
        assertThat(p.getLeader()).isEqualTo(1); // first surviving ISR member
        assertThat(p.getIsrList()).containsExactlyInAnyOrder(1, 3);
        assertThat(p.getLeaderEpoch()).isEqualTo(6);
    }

    @Test
    void staleFollowerThatIsNotLeaderProducesNoProposal() {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 3, 0L);
        tm.onPartitionChange("t", 0, /*leader*/ 1, List.of(1, 2, 3), List.of(1, 2, 3), 5, 0);

        var liveness = new BrokerLiveness();
        liveness.recordSignal(1, 0L, 10_000_000_000L);
        liveness.recordSignal(2, 0L, 0L); // stale but not a leader
        liveness.recordSignal(3, 0L, 10_000_000_000L);

        var proposer = new CapturingProposer();
        var fencer = new BrokerFencer(1, tm, liveness, proposer, () -> Role.LEADER, staleThresholdNanos());

        fencer.tick(10_000_000_000L);

        // Broker 2 doesn't lead any partition, so no PartitionChangeRecord
        // proposal is needed. (P6.5.b/c does not model ISR shrink-on-fence.)
        assertThat(proposer.proposals).isEmpty();
    }

    @Test
    void noSurvivingIsrMemberEmitsSentinelLeader() {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 1, 0L);
        tm.onPartitionChange("t", 0, /*leader*/ 2, List.of(2), List.of(2), 5, 0);

        var liveness = new BrokerLiveness();
        liveness.recordSignal(2, 0L, 0L);

        var proposer = new CapturingProposer();
        var fencer = new BrokerFencer(1, tm, liveness, proposer, () -> Role.LEADER, staleThresholdNanos());

        fencer.tick(10_000_000_000L);

        assertThat(proposer.proposals).hasSize(1);
        assertThat(proposer.proposals.get(0).getLeader()).isEqualTo(-1);
        assertThat(proposer.proposals.get(0).getIsrList()).isEmpty();
    }

    @Test
    void tickWhenNotLeaderIsNoOp() {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 3, 0L);
        tm.onPartitionChange("t", 0, 2, List.of(2, 1, 3), List.of(2, 1, 3), 5, 0);

        var liveness = new BrokerLiveness();
        liveness.recordSignal(2, 0L, 0L); // stale

        var proposer = new CapturingProposer();
        var fencer = new BrokerFencer(1, tm, liveness, proposer, () -> Role.FOLLOWER, staleThresholdNanos());

        fencer.tick(10_000_000_000L);
        assertThat(proposer.proposals).isEmpty();
    }

    @Test
    void selfIsNeverFenced() {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 3, 0L);
        tm.onPartitionChange("t", 0, /*self-leader*/ 1, List.of(1, 2, 3), List.of(1, 2, 3), 5, 0);

        var liveness = new BrokerLiveness();
        liveness.recordSignal(1, 0L, 0L); // self looks stale — nonsense but possible under clock skew
        liveness.recordSignal(2, 0L, 10_000_000_000L);
        liveness.recordSignal(3, 0L, 10_000_000_000L);

        var proposer = new CapturingProposer();
        var fencer = new BrokerFencer(1, tm, liveness, proposer, () -> Role.LEADER, staleThresholdNanos());

        fencer.tick(10_000_000_000L);
        assertThat(proposer.proposals).isEmpty();
    }

    @Test
    void healthyClusterProducesNoProposals() {
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 2, 3, 0L);
        tm.onPartitionChange("t", 0, 1, List.of(1, 2, 3), List.of(1, 2, 3), 5, 0);
        tm.onPartitionChange("t", 1, 2, List.of(2, 1, 3), List.of(2, 1, 3), 5, 0);

        var liveness = new BrokerLiveness();
        liveness.recordSignal(1, 0L, 10_000_000_000L);
        liveness.recordSignal(2, 0L, 10_000_000_000L);
        liveness.recordSignal(3, 0L, 10_000_000_000L);

        var proposer = new CapturingProposer();
        var fencer = new BrokerFencer(1, tm, liveness, proposer, () -> Role.LEADER, staleThresholdNanos());

        fencer.tick(10_000_000_000L);
        assertThat(proposer.proposals).isEmpty();
    }

    @Test
    void staleBrokerWithoutLivenessEntryIsIgnored() {
        // A broker in the replica set but never observed by the controller
        // yet (e.g., controller just elected, other brokers haven't sent
        // their first heartbeat). Don't fence on first sight — wait for
        // the grace period (= staleThreshold) to elapse first.
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 3, 0L);
        tm.onPartitionChange("t", 0, 2, List.of(2, 1, 3), List.of(2, 1, 3), 5, 0);

        var liveness = new BrokerLiveness();
        // Only broker 1 has a signal.
        liveness.recordSignal(1, 0L, 10_000_000_000L);

        var proposer = new CapturingProposer();
        var fencer = new BrokerFencer(1, tm, liveness, proposer, () -> Role.LEADER, staleThresholdNanos());

        fencer.tick(10_000_000_000L);
        assertThat(proposer.proposals).isEmpty();
    }

    @Test
    void neverSeenLeaderIsFencedAfterGracePeriod() {
        // Root cause of the MultiBrokerFailoverIT flake: a partition leader
        // that dies before its first heartbeat ever reaches the controller
        // has NO liveness entry, and the fencer skipped it forever. The
        // fencer must start a grace clock at first observation and fence
        // once staleThreshold elapses with still no signal.
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 3, 0L);
        tm.onPartitionChange("t", 0, /*leader*/ 2, List.of(2, 1, 3), List.of(2, 1, 3), /*leaderEpoch*/ 5, 0);

        var liveness = new BrokerLiveness();
        liveness.recordSignal(1, 0L, 10_000_000_000L);
        liveness.recordSignal(3, 0L, 10_000_000_000L);
        // Broker 2: no entry at all — never heard from.

        var proposer = new CapturingProposer();
        var fencer = new BrokerFencer(1, tm, liveness, proposer, () -> Role.LEADER, staleThresholdNanos());

        fencer.tick(10_000_000_000L); // first observation: seeds grace clock
        assertThat(proposer.proposals).isEmpty();

        fencer.tick(12_000_000_000L); // 2s in: still within grace
        assertThat(proposer.proposals).isEmpty();

        fencer.tick(13_100_000_000L); // 3.1s in: grace elapsed → fence
        assertThat(proposer.proposals).hasSize(1);
        var p = proposer.proposals.get(0);
        assertThat(p.getTopic()).isEqualTo("t");
        assertThat(p.getPartition()).isEqualTo(0);
        assertThat(p.getLeader()).isEqualTo(1);
        assertThat(p.getIsrList()).containsExactlyInAnyOrder(1, 3);
        assertThat(p.getLeaderEpoch()).isEqualTo(6);
    }

    @Test
    void heartbeatDuringGracePeriodCancelsNeverSeenFencing() {
        // If the missing broker's first heartbeat lands mid-grace, the
        // grace clock is discarded and normal staleness rules apply.
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 3, 0L);
        tm.onPartitionChange("t", 0, /*leader*/ 2, List.of(2, 1, 3), List.of(2, 1, 3), 5, 0);

        var liveness = new BrokerLiveness();
        liveness.recordSignal(1, 0L, 10_000_000_000L);
        liveness.recordSignal(3, 0L, 10_000_000_000L);

        var proposer = new CapturingProposer();
        var fencer = new BrokerFencer(1, tm, liveness, proposer, () -> Role.LEADER, staleThresholdNanos());

        fencer.tick(10_000_000_000L); // seeds grace clock for broker 2
        liveness.recordSignal(2, 0L, 12_000_000_000L); // first heartbeat lands

        fencer.tick(13_100_000_000L); // grace would have elapsed, but the
        // signal is only 1.1s old → fresh → no fence
        assertThat(proposer.proposals).isEmpty();

        fencer.tick(15_100_000_000L); // signal now 3.1s old → normal staleness path fences
        assertThat(proposer.proposals).hasSize(1);
    }

    @Test
    void losingLeadershipResetsNeverSeenGraceClocks() {
        // Grace clocks measure continuous observation *as controller*. A
        // fencer that loses leadership must restart the grace period when
        // it becomes leader again, not fence instantly off a stale clock.
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 3, 0L);
        tm.onPartitionChange("t", 0, /*leader*/ 2, List.of(2, 1, 3), List.of(2, 1, 3), 5, 0);

        var liveness = new BrokerLiveness();
        liveness.recordSignal(1, 0L, 10_000_000_000L);
        liveness.recordSignal(3, 0L, 10_000_000_000L);

        var role = new java.util.concurrent.atomic.AtomicReference<>(Role.LEADER);
        var proposer = new CapturingProposer();
        var fencer = new BrokerFencer(1, tm, liveness, proposer, role::get, staleThresholdNanos());

        fencer.tick(10_000_000_000L); // seeds grace clock
        role.set(Role.FOLLOWER);
        fencer.tick(11_000_000_000L); // not leader: clocks reset
        role.set(Role.LEADER);

        fencer.tick(13_100_000_000L); // re-seeded here, grace not yet elapsed
        assertThat(proposer.proposals).isEmpty();

        fencer.tick(16_300_000_000L); // 3.2s after re-seed → fence
        assertThat(proposer.proposals).hasSize(1);
    }

    @Test
    void sentinelNoLeaderPartitionIsNeverReFenced() {
        // Regression guard for a bug in the never-seen grace clock: after a
        // fence legitimately set leader=-1 (no surviving ISR), the fencer
        // treated "-1" as a never-heard-from broker, seeded a grace clock
        // for it, and re-fenced the partition every tick — inflating
        // leader_epoch by ~1/tick (observed epochs 2..36 in one IT run).
        var tm = new TopicManager();
        tm.onTopicCommitted("t", 1, 1, 0L);
        tm.onPartitionChange("t", 0, /*leader*/ -1, List.of(), List.of(2), /*leaderEpoch*/ 1, 0);

        var liveness = new BrokerLiveness();
        liveness.recordSignal(1, 0L, 10_000_000_000L);

        var proposer = new CapturingProposer();
        var fencer = new BrokerFencer(1, tm, liveness, proposer, () -> Role.LEADER, staleThresholdNanos());

        fencer.tick(10_000_000_000L);
        fencer.tick(15_000_000_000L); // well past the grace threshold
        fencer.tick(20_000_000_000L);

        assertThat(proposer.proposals)
                .as("a partition already at the no-leader sentinel must not be re-fenced")
                .isEmpty();
    }
}
