package jbroker.broker;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import jbroker.proto.raft.MetadataRecord;
import jbroker.raft.core.LogEntry;
import jbroker.raft.core.StateMachine;

/**
 * Raft state machine that interprets committed {@code MetadataRecord}
 * payloads and forwards them to an in-memory {@link TopicManager}. Replayed
 * on startup to recover the full topic catalogue from the Raft log.
 */
public final class MetadataStateMachine implements StateMachine {

    /**
     * Notified whenever a partition's {@code leader_epoch} strictly
     * increases on this broker. Partition-epoch-only flips (ISR shrink/
     * expand under the same leader) do NOT fire. Used by the follower
     * replication path to record {@code LeaderEpochCheckpoint} entries.
     */
    @FunctionalInterface
    public interface LeaderEpochListener {
        void onLeaderEpochBump(String topic, int partition, int newLeaderEpoch, int leaderBrokerId);
    }

    /**
     * Fires after every applied {@code PartitionChangeRecord}, passing the
     * post-merge partition state. Note: "applied" means "the apply call
     * ran"; {@link TopicManager}'s accept-if-newer merge may keep the
     * existing state if the record is older, in which case the listener
     * still fires with the unchanged state. Consumers (e.g. {@code
     * ReplicaFetcherManager}) must be idempotent under re-firing.
     */
    @FunctionalInterface
    public interface PartitionChangeListener {
        void onPartitionChange(String topic, int partition, PartitionState state);
    }

    /** Fires after every applied {@code BrokerRegistrationRecord}. */
    @FunctionalInterface
    public interface BrokerRegistrationListener {
        void onBrokerRegistration(int brokerId, String host, int port);
    }

    private final TopicManager topicManager;
    private final ProducerIdRegistry producerIdRegistry;
    private final LeaderEpochListener leaderEpochListener;
    private final BrokerRegistrationListener brokerRegistrationListener;
    private final PartitionChangeListener partitionChangeListener;

    public MetadataStateMachine(TopicManager topicManager) {
        this(topicManager, new ProducerIdRegistry(), (t, p, e, l) -> {}, (b, h, pt) -> {}, (t, p, s) -> {});
    }

    public MetadataStateMachine(TopicManager topicManager, ProducerIdRegistry producerIdRegistry) {
        this(topicManager, producerIdRegistry, (t, p, e, l) -> {}, (b, h, pt) -> {}, (t, p, s) -> {});
    }

    public MetadataStateMachine(
            TopicManager topicManager, ProducerIdRegistry producerIdRegistry, LeaderEpochListener leaderEpochListener) {
        this(topicManager, producerIdRegistry, leaderEpochListener, (b, h, pt) -> {}, (t, p, s) -> {});
    }

    public MetadataStateMachine(
            TopicManager topicManager,
            ProducerIdRegistry producerIdRegistry,
            LeaderEpochListener leaderEpochListener,
            BrokerRegistrationListener brokerRegistrationListener,
            PartitionChangeListener partitionChangeListener) {
        this.topicManager = topicManager;
        this.producerIdRegistry = producerIdRegistry;
        this.leaderEpochListener = leaderEpochListener;
        this.brokerRegistrationListener = brokerRegistrationListener;
        this.partitionChangeListener = partitionChangeListener;
    }

    @Override
    public void apply(LogEntry entry) {
        if (entry.type() != LogEntry.Type.NORMAL || entry.payload().length == 0) {
            return;
        }
        MetadataRecord record;
        try {
            record = MetadataRecord.parseFrom(entry.payload());
        } catch (InvalidProtocolBufferException e) {
            // A non-metadata payload on the metadata Raft log — skip.
            return;
        }
        if (record.hasTopic()) {
            var t = record.getTopic();
            topicManager.onTopicCommitted(
                    t.getTopic(),
                    t.getPartitions(),
                    t.getReplicationFactor(),
                    t.getCreatedMillis(),
                    t.getInternal(),
                    t.getCompact());
        } else if (record.hasPartitionChange()) {
            applyPartitionChange(record.getPartitionChange());
        } else if (record.hasCreateTopic()) {
            var ct = record.getCreateTopic();
            var t = ct.getTopic();
            topicManager.onTopicCommitted(
                    t.getTopic(),
                    t.getPartitions(),
                    t.getReplicationFactor(),
                    t.getCreatedMillis(),
                    t.getInternal(),
                    t.getCompact());
            for (var p : ct.getPartitionChangesList()) {
                applyPartitionChange(p);
            }
        } else if (record.hasProducerIdAssignment()) {
            // P6.7: advance the producer-id counter. ProducerIdRegistry is
            // idempotent under replay — a duplicate or out-of-order apply
            // cannot regress the counter.
            producerIdRegistry.applyAssignment(record.getProducerIdAssignment().getNextProducerId());
        } else if (record.hasBroker()) {
            // P6.5.a: broker-gRPC address discovery. The listener is where
            // BrokerRegistry + ReplicaFetcherManager plug in.
            var r = record.getBroker();
            brokerRegistrationListener.onBrokerRegistration(r.getBrokerId(), r.getHost(), r.getPort());
        }
        // PartitionRecord: not yet dispatched — unused by any consumer.
    }

    private void applyPartitionChange(jbroker.proto.raft.PartitionChangeRecord p) {
        // Empty replicas list = pre-P6.3 record; default to ISR for
        // backward compatibility (same replica set either way).
        var replicas = p.getReplicasList().isEmpty() ? p.getIsrList() : p.getReplicasList();
        int priorLeaderEpoch = topicManager
                .partitionState(p.getTopic(), p.getPartition())
                .map(PartitionState::leaderEpoch)
                .orElse(-1);
        topicManager.onPartitionChange(
                p.getTopic(),
                p.getPartition(),
                p.getLeader(),
                p.getIsrList(),
                replicas,
                p.getLeaderEpoch(),
                p.getPartitionEpoch());
        // Fire only on a real bump — merge may have ignored this record if
        // its epoch didn't advance, and partition-epoch-only flips under the
        // same leader_epoch mustn't trigger P6.4 follower reconciliation.
        if (p.getLeaderEpoch() > priorLeaderEpoch) {
            leaderEpochListener.onLeaderEpochBump(p.getTopic(), p.getPartition(), p.getLeaderEpoch(), p.getLeader());
        }
        // Partition-change listener fires unconditionally with the
        // post-merge state. Consumers reconcile idempotently on every fire.
        topicManager
                .partitionState(p.getTopic(), p.getPartition())
                .ifPresent(s -> partitionChangeListener.onPartitionChange(p.getTopic(), p.getPartition(), s));
    }

    // Snapshot format versions.
    //  v1 (pre-P6.1): topics only.
    //  v2 (P6.1):     adds partition-state section (leader, ISR, epoch).
    //  v3 (P6.3):     adds replica set alongside ISR.
    //  v4 (P6.3):     splits leader_epoch from partition_epoch.
    //  v5 (P6.7):     appends producer-id counter.
    //  v6 (P7.2):     per-topic internal + compact flags.
    private static final byte SNAPSHOT_VERSION = 6;

    @Override
    public void snapshot(OutputStream out) throws IOException {
        var dout = new java.io.DataOutputStream(out);
        dout.writeByte(SNAPSHOT_VERSION);
        // v6 snapshot includes internal topics (Phase 7's __consumer_offsets);
        // listAll() returns the full catalogue while list() filters internals.
        var list = topicManager.listAll();
        dout.writeInt(list.size());
        for (var t : list) {
            dout.writeUTF(t.topic());
            dout.writeInt(t.partitions());
            dout.writeInt(t.replicationFactor());
            dout.writeLong(t.createdMillis());
            // v6: internal + compact flags. Older readers only see the four
            // fields above; v6 readers additionally consume two bools.
            dout.writeBoolean(t.internal());
            dout.writeBoolean(t.compact());
        }
        var assignments = topicManager.allPartitionAssignments();
        dout.writeInt(assignments.size());
        for (var a : assignments) {
            dout.writeUTF(a.topic());
            dout.writeInt(a.partition());
            dout.writeInt(a.state().leader());
            var isr = a.state().isr();
            dout.writeInt(isr.size());
            for (int b : isr) {
                dout.writeInt(b);
            }
            dout.writeInt(a.state().leaderEpoch());
            var replicas = a.state().replicas();
            dout.writeInt(replicas.size());
            for (int b : replicas) {
                dout.writeInt(b);
            }
            dout.writeInt(a.state().partitionEpoch());
        }
        // v5: producer-id counter.
        dout.writeLong(producerIdRegistry.peekNextProducerId());
        dout.flush();
    }

    @Override
    public void restore(InputStream in) throws IOException {
        var din = new java.io.DataInputStream(in);
        int version = din.readByte() & 0xff;
        if (version < 1 || version > SNAPSHOT_VERSION) {
            throw new IOException("unsupported metadata snapshot version: " + version);
        }
        int n = din.readInt();
        for (int i = 0; i < n; i++) {
            var topic = din.readUTF();
            var partitions = din.readInt();
            var rf = din.readInt();
            var created = din.readLong();
            boolean internal;
            boolean compact;
            if (version >= 6) {
                internal = din.readBoolean();
                compact = din.readBoolean();
            } else {
                // Pre-P7 snapshots predate the flags. Names beginning with
                // "__" are still treated as internal so legacy snapshots
                // restored against new code don't accidentally surface
                // plumbing topics in Admin.ListTopics.
                internal = topic.startsWith("__");
                compact = false;
            }
            topicManager.onTopicCommitted(topic, partitions, rf, created, internal, compact);
        }
        if (version >= 2) {
            int m = din.readInt();
            for (int i = 0; i < m; i++) {
                var topic = din.readUTF();
                var partition = din.readInt();
                var leader = din.readInt();
                int isrSize = din.readInt();
                var isr = new java.util.ArrayList<Integer>(isrSize);
                for (int j = 0; j < isrSize; j++) {
                    isr.add(din.readInt());
                }
                var epoch = din.readInt();
                java.util.List<Integer> replicas;
                if (version >= 3) {
                    int replicasSize = din.readInt();
                    var r = new java.util.ArrayList<Integer>(replicasSize);
                    for (int j = 0; j < replicasSize; j++) {
                        r.add(din.readInt());
                    }
                    replicas = r;
                } else {
                    replicas = isr;
                }
                int partitionEpoch = version >= 4 ? din.readInt() : 0;
                topicManager.onPartitionChange(topic, partition, leader, isr, replicas, epoch, partitionEpoch);
            }
        }
        if (version >= 5) {
            producerIdRegistry.applyAssignment(din.readLong());
        }
    }
}
