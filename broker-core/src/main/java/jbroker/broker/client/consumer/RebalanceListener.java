package jbroker.broker.client.consumer;

import java.util.Collection;
import jbroker.proto.common.TopicPartition;

/**
 * Notified by the {@link Consumer} when its partition assignment changes.
 *
 * <p>{@link #onPartitionsRevoked} fires when the coordinator instructs the
 * consumer to release partitions (during the kept-set stage of a
 * cooperative rebalance, or unconditionally on close/leave). The
 * application should commit any in-flight progress for those partitions
 * before returning — the next heartbeat carries
 * {@code owned_partitions} reflecting the post-revoke state, signalling
 * the coordinator that revocation is complete.
 *
 * <p>{@link #onPartitionsAssigned} fires when newly-added partitions
 * arrive (stage 2 of a cooperative rebalance, or initial assignment on
 * first join). The application can prime any per-partition state it
 * needs.
 */
public interface RebalanceListener {

    void onPartitionsRevoked(Collection<TopicPartition> revoked);

    void onPartitionsAssigned(Collection<TopicPartition> added);

    /** No-op listener — useful when the application doesn't need callbacks. */
    RebalanceListener NO_OP = new RebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> revoked) {}

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> added) {}
    };
}
