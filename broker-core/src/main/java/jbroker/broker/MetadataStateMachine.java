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

    private final TopicManager topicManager;
    private final ProducerIdRegistry producerIdRegistry;

    public MetadataStateMachine(TopicManager topicManager) {
        this(topicManager, new ProducerIdRegistry());
    }

    public MetadataStateMachine(TopicManager topicManager, ProducerIdRegistry producerIdRegistry) {
        this.topicManager = topicManager;
        this.producerIdRegistry = producerIdRegistry;
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
                    t.getTopic(), t.getPartitions(), t.getReplicationFactor(), t.getCreatedMillis());
        } else if (record.hasPartitionChange()) {
            applyPartitionChange(record.getPartitionChange());
        } else if (record.hasCreateTopic()) {
            var ct = record.getCreateTopic();
            var t = ct.getTopic();
            topicManager.onTopicCommitted(
                    t.getTopic(), t.getPartitions(), t.getReplicationFactor(), t.getCreatedMillis());
            for (var p : ct.getPartitionChangesList()) {
                applyPartitionChange(p);
            }
        } else if (record.hasProducerIdAssignment()) {
            // P6.7: advance the producer-id counter. ProducerIdRegistry is
            // idempotent under replay — a duplicate or out-of-order apply
            // cannot regress the counter.
            producerIdRegistry.applyAssignment(record.getProducerIdAssignment().getNextProducerId());
        }
        // PartitionRecord / BrokerRegistrationRecord: not yet dispatched;
        // Phase 6 later steps will consume them.
    }

    private void applyPartitionChange(jbroker.proto.raft.PartitionChangeRecord p) {
        // Empty replicas list = pre-P6.3 record; default to ISR for
        // backward compatibility (same replica set either way).
        var replicas = p.getReplicasList().isEmpty() ? p.getIsrList() : p.getReplicasList();
        topicManager.onPartitionChange(
                p.getTopic(),
                p.getPartition(),
                p.getLeader(),
                p.getIsrList(),
                replicas,
                p.getLeaderEpoch(),
                p.getPartitionEpoch());
    }

    // Snapshot format versions.
    //  v1 (pre-P6.1): topics only.
    //  v2 (P6.1):     adds partition-state section (leader, ISR, epoch).
    //  v3 (P6.3):     adds replica set alongside ISR.
    //  v4 (P6.3):     splits leader_epoch from partition_epoch.
    //  v5 (P6.7):     appends producer-id counter.
    private static final byte SNAPSHOT_VERSION = 5;

    @Override
    public void snapshot(OutputStream out) throws IOException {
        var dout = new java.io.DataOutputStream(out);
        dout.writeByte(SNAPSHOT_VERSION);
        var list = topicManager.list();
        dout.writeInt(list.size());
        for (var t : list) {
            dout.writeUTF(t.topic());
            dout.writeInt(t.partitions());
            dout.writeInt(t.replicationFactor());
            dout.writeLong(t.createdMillis());
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
            topicManager.onTopicCommitted(topic, partitions, rf, created);
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
