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
        }
        // Partition / broker records: recorded on the log but no action
        // needed in Phase 5's single-node broker (no replication, self is
        // always the sole replica). Phase 6 will dispatch them.
    }

    @Override
    public void snapshot(OutputStream out) throws IOException {
        var dout = new java.io.DataOutputStream(out);
        var list = topicManager.list();
        dout.writeInt(list.size());
        for (var t : list) {
            dout.writeUTF(t.topic());
            dout.writeInt(t.partitions());
            dout.writeInt(t.replicationFactor());
            dout.writeLong(t.createdMillis());
        }
        dout.flush();
    }

    @Override
    public void restore(InputStream in) throws IOException {
        var din = new java.io.DataInputStream(in);
        int n = din.readInt();
        for (int i = 0; i < n; i++) {
            var topic = din.readUTF();
            var partitions = din.readInt();
            var rf = din.readInt();
            var created = din.readLong();
            topicManager.onTopicCommitted(topic, partitions, rf, created);
        }
    }
}
