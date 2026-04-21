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

    public MetadataStateMachine(TopicManager topicManager) {
        this.topicManager = topicManager;
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
        }
        // PartitionRecord / BrokerRegistrationRecord: not yet dispatched;
        // Phase 6 later steps will consume them.
    }

    private void applyPartitionChange(jbroker.proto.raft.PartitionChangeRecord p) {
        topicManager.onPartitionChange(
                p.getTopic(), p.getPartition(), p.getLeader(), p.getIsrList(), p.getLeaderEpoch());
    }

    // Snapshot format version. v1 = topics only (pre-P6.1). v2 adds the
    // partition-state section — topics carry leader/ISR/epoch now, which
    // would otherwise silently vanish across an InstallSnapshot round-trip
    // and cause every subsequent produce to return NOT_LEADER.
    private static final byte SNAPSHOT_VERSION = 2;

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
        }
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
                topicManager.onPartitionChange(topic, partition, leader, isr, epoch);
            }
        }
    }
}
