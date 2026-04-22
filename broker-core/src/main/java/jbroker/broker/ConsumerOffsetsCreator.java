package jbroker.broker;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import jbroker.proto.raft.CreateTopicRecord;
import jbroker.proto.raft.MetadataRecord;
import jbroker.proto.raft.PartitionChangeRecord;
import jbroker.proto.raft.TopicRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Milestone 7.2 — proposes the {@code __consumer_offsets} {@code CreateTopicRecord}
 * exactly once per cluster, when the active controller observes that the
 * topic doesn't yet exist. Idempotent: callers may invoke {@link #ensureCreated()}
 * on every controller tick without producing duplicate proposals.
 *
 * <p>Replication factor is clamped to {@code min(3, |knownBrokers|)} so a
 * single-broker cluster (e.g. existing P5/P6 ITs) doesn't deadlock waiting
 * for absent followers. The fixed 50-partition shape matches Kafka
 * convention; changing it would invalidate every committed offset's
 * coordinator routing (see spec §"`__consumer_offsets` internal topic").
 */
public final class ConsumerOffsetsCreator {

    private static final Logger log = LoggerFactory.getLogger(ConsumerOffsetsCreator.class);

    /** How a propose-and-wait is dispatched — same contract as {@link AdminHandler.MetadataProposer}. */
    public interface Proposer {
        void propose(byte[] payload) throws Exception;
    }

    private final TopicManager topicManager;
    private final Supplier<Set<Integer>> knownBrokers;
    private final int selfBrokerId;
    private final Proposer proposer;
    private final Supplier<Boolean> isLeader;
    private final int partitions;
    private volatile boolean proposed;

    /** Convenience constructor — uses the canonical {@link ConsumerOffsetsTopic#PARTITION_COUNT}. */
    public ConsumerOffsetsCreator(
            TopicManager topicManager,
            Supplier<Set<Integer>> knownBrokers,
            int selfBrokerId,
            Proposer proposer,
            Supplier<Boolean> isLeader) {
        this(topicManager, knownBrokers, selfBrokerId, proposer, isLeader, ConsumerOffsetsTopic.PARTITION_COUNT);
    }

    public ConsumerOffsetsCreator(
            TopicManager topicManager,
            Supplier<Set<Integer>> knownBrokers,
            int selfBrokerId,
            Proposer proposer,
            Supplier<Boolean> isLeader,
            int partitions) {
        if (partitions < 1) {
            throw new IllegalArgumentException("partitions must be ≥ 1, got " + partitions);
        }
        this.topicManager = topicManager;
        this.knownBrokers = knownBrokers;
        this.selfBrokerId = selfBrokerId;
        this.proposer = proposer;
        this.isLeader = isLeader;
        this.partitions = partitions;
    }

    /**
     * Idempotent — callable from a tick scheduler. Returns true if the topic
     * exists in {@link TopicManager} (whether created here or already
     * present), false if the controller still needs to retry (not leader, no
     * peers known yet, propose failure, etc.).
     */
    public boolean ensureCreated() {
        if (topicManager.exists(ConsumerOffsetsTopic.NAME)) {
            proposed = true;
            return true;
        }
        if (proposed) {
            // Already in flight; the apply hasn't happened yet. Let the
            // next tick re-check exists().
            return false;
        }
        if (!isLeader.get()) return false;

        var brokers = knownBrokers.get();
        if (brokers.isEmpty()) return false;
        // candidates: self first, then peers in id order so the assignment
        // is deterministic across leader changes.
        var candidates = new java.util.ArrayList<Integer>();
        candidates.add(selfBrokerId);
        var sortedPeers = new java.util.TreeSet<>(brokers);
        sortedPeers.remove(selfBrokerId);
        candidates.addAll(sortedPeers);
        int rf = Math.min(3, candidates.size());
        var replicas = candidates.subList(0, rf);

        var ct = CreateTopicRecord.newBuilder()
                .setTopic(TopicRecord.newBuilder()
                        .setTopic(ConsumerOffsetsTopic.NAME)
                        .setPartitions(partitions)
                        .setReplicationFactor(rf)
                        .setCreatedMillis(System.currentTimeMillis())
                        .setInternal(true)
                        .setCompact(true)
                        .build());
        // Round-robin partition leadership across the chosen replica set so
        // not every coordinator partition lands on the controller.
        for (int p = 0; p < partitions; p++) {
            int leader = replicas.get(p % replicas.size());
            var pc = PartitionChangeRecord.newBuilder()
                    .setTopic(ConsumerOffsetsTopic.NAME)
                    .setPartition(p)
                    .setLeader(leader)
                    .setLeaderEpoch(0);
            // ISR ordering: leader first, then the rest of the replica set
            // in id order (matches the AdminHandler invariant).
            pc.addIsr(leader);
            for (int r : replicas) {
                if (r != leader) pc.addIsr(r);
            }
            for (int r : replicas) {
                pc.addReplicas(r);
            }
            ct.addPartitionChanges(pc);
        }
        var record = MetadataRecord.newBuilder().setCreateTopic(ct.build()).build();
        try {
            proposer.propose(record.toByteArray());
            proposed = true;
            return true;
        } catch (Exception e) {
            log.debug("__consumer_offsets propose failed; will retry on next tick", e);
            return false;
        }
    }

    /** Test-only — clears the in-flight flag so a fresh attempt can be triggered. */
    void resetForTest() {
        proposed = false;
    }

    /** Convenience: a {@link Proposer} backed by an {@link AdminHandler.MetadataProposer}. */
    public static Proposer fromMetadataProposer(AdminHandler.MetadataProposer proposer) {
        return payload -> proposer.proposeAndWait(payload, TimeUnit.SECONDS.toMillis(10));
    }
}
