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
        // their first heartbeat). Don't fence — wait for real evidence.
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
}
