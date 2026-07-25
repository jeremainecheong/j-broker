/**
 * Transaction coordinator core: a pure state machine over the
 * {@code __transaction_state} compacted topic, mirroring the structure of
 * {@link jbroker.broker.group.GroupCoordinator} over
 * {@code __consumer_offsets}. The coordinator for a transactional_id is
 * the leader of {@code __transaction_state} partition
 * {@code Math.floorMod(txnId.hashCode(), partitionCount)}; on gaining
 * leadership the broker replays the partition into a fresh
 * {@link jbroker.broker.txn.TxnCoordinator} via
 * {@link jbroker.broker.txn.TxnStateRecovery} (latest record per key wins,
 * exactly what compaction preserves).
 *
 * <p>The core never performs I/O. Every input returns the state record the
 * caller must append to the coordinator partition and/or the marker
 * instructions the caller must deliver — the same returns-effects
 * philosophy as {@code jbroker.raft.core.RaftCore#step}. The wiring owns
 * ordering: append the returned record (with acks=all) before answering
 * the client.
 *
 * <h2>Two-phase contract</h2>
 *
 * <p>A transaction's outcome is decided exactly once, at the moment a
 * {@code PREPARE_COMMIT} or {@code PREPARE_ABORT} record becomes durable in
 * {@code __transaction_state}:
 *
 * <ul>
 *   <li><b>After Prepare*</b> the outcome is irrevocable. The record
 *       carries the full partition set, producer id/epoch, and the
 *       decision, so any coordinator — including one elected after a
 *       crash — can regenerate the exact marker instructions
 *       ({@link jbroker.broker.txn.TxnCoordinator#pendingMarkers}) and
 *       (re)deliver them. Marker delivery is idempotent at the data
 *       partitions, so redelivery after partial failure is safe. Retried
 *       {@code EndTxn} calls with the matching outcome return the same
 *       instructions instead of erroring.</li>
 *   <li><b>After Complete*</b> every marker is known to sit in the data
 *       logs. Only then may the coordinator forget the partition set —
 *       compaction is free to drop the superseded records for the key.</li>
 * </ul>
 *
 * <p>Markers are delivered <em>before</em> {@code COMPLETE_*} is logged,
 * never after: the Complete record is the assertion that delivery
 * finished. If Complete were logged first and the coordinator crashed
 * mid-delivery, the obligation to write the remaining markers would exist
 * nowhere — the replayed state would look finished, the undelivered
 * partitions would hold an open transaction forever, and the last stable
 * offset on those partitions would never advance. Logging Complete last
 * means a crash can only leave the benign dual: markers delivered but
 * still marked Prepare*, which redelivery resolves.
 *
 * <p>Zombie fencing: {@code InitTransactions} bumps the producer epoch,
 * so every input carrying an older (or unknown-future) epoch answers
 * {@code PRODUCER_FENCED}. A transaction abandoned by a fenced or dead
 * producer is aborted either by the epoch bump itself (in-flight
 * transaction at init) or by the timeout sweep
 * ({@link jbroker.broker.txn.TxnCoordinator#tick}).
 */
package jbroker.broker.txn;
