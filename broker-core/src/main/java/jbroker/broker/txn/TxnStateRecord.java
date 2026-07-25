package jbroker.broker.txn;

import java.util.List;
import jbroker.proto.common.TopicPartition;

/**
 * One compacted-topic entry for a transactional_id — the unit both the
 * live path appends and the replay walk consumes. Key on disk is the
 * transactional_id (see {@link TxnStateTopic#keyForTxn}); everything else
 * rides in the value. Latest record per key fully describes the id's
 * state, so compaction keeping only the newest record loses nothing.
 *
 * @param transactionalId compaction key; routes to the coordinator partition
 * @param producerId stable per-transactional_id producer id
 * @param producerEpoch current fencing epoch, bumped by InitTransactions
 * @param state where the current transaction sits in its lifecycle
 * @param partitions partitions the current transaction wrote to; the
 *     marker fan-out set for Prepare* records
 * @param timeoutMs inactivity budget before the coordinator aborts an
 *     ONGOING transaction
 * @param lastUpdateMs wall-clock millis of the last accepted input;
 *     the timeout sweep measures from here. Survives failover as-is —
 *     an expired transaction gets aborted on the new coordinator's
 *     first sweep.
 */
public record TxnStateRecord(
        String transactionalId,
        long producerId,
        int producerEpoch,
        TxnState state,
        List<TopicPartition> partitions,
        int timeoutMs,
        long lastUpdateMs) {

    public TxnStateRecord {
        partitions = List.copyOf(partitions);
    }
}
