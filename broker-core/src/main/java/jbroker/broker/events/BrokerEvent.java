package jbroker.broker.events;

import java.util.List;

/**
 * Sealed hierarchy of broker-side events surfaced over the Milestone 8 SSE
 * stream. Each event carries an {@code id} allocated at publish time; the
 * id is strictly monotonic within a single broker process so the admin-app
 * can honour {@code Last-Event-ID} replays across reconnects.
 *
 * <p>Milestone 8 scope: leader-change + Raft-term + broker-registration/fence +
 * group-rebalance. Remaining the spec event types (ISR shrink/expand,
 * raft_commit, throughput_sample, chaos_action) arrive in Milestone 9.
 */
public sealed interface BrokerEvent {

    long id();

    /** Short type name used as the SSE {@code event:} field. */
    String typeName();

    record LeaderChanged(long id, String topic, int partition, int oldLeader, int newLeader, int leaderEpoch)
            implements BrokerEvent {
        @Override
        public String typeName() {
            return "leader_changed";
        }
    }

    record IsrChanged(long id, String topic, int partition, List<Integer> isr, boolean shrink) implements BrokerEvent {
        public IsrChanged {
            isr = List.copyOf(isr);
        }

        @Override
        public String typeName() {
            return shrink ? "isr_shrink" : "isr_expand";
        }
    }

    record RaftTermChanged(long id, long oldTerm, long newTerm, int newLeader) implements BrokerEvent {
        @Override
        public String typeName() {
            return "raft_term_change";
        }
    }

    record BrokerRegistered(long id, int brokerId, String host, int port) implements BrokerEvent {
        @Override
        public String typeName() {
            return "broker_registered";
        }
    }

    record BrokerFenced(long id, int brokerId) implements BrokerEvent {
        @Override
        public String typeName() {
            return "broker_fenced";
        }
    }

    record ConsumerGroupRebalance(long id, String groupId, int generation, int memberCount) implements BrokerEvent {
        @Override
        public String typeName() {
            return "consumer_group_rebalance";
        }
    }
}
