package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import jbroker.proto.raft.AclRecord;
import jbroker.proto.raft.MetadataRecord;
import jbroker.proto.raft.RemoveAclRecord;
import jbroker.raft.core.LogEntry;
import jbroker.raft.core.Term;
import org.junit.jupiter.api.Test;

class MetadataStateMachineAclTest {

    @Test
    void appliedAclRecordsLandInTheStore() {
        var sm = new MetadataStateMachine(new TopicManager());

        sm.apply(aclApply("alice", "topic", "orders", false, "produce", true));

        assertThat(sm.aclStore().allows("alice", "topic", "orders", "produce")).isTrue();
    }

    @Test
    void removeAclRecordDeletesByIdentityKey() {
        var sm = new MetadataStateMachine(new TopicManager());
        sm.apply(aclApply("alice", "topic", "orders", false, "produce", true));

        sm.apply(removeAclApply("alice", "topic", "orders", false, "produce"));

        assertThat(sm.aclStore().list()).isEmpty();
    }

    @Test
    void replayIsIdempotent() {
        var sm = new MetadataStateMachine(new TopicManager());
        var entry = aclApply("alice", "topic", "orders", false, "produce", true);

        sm.apply(entry);
        sm.apply(entry);
        sm.apply(removeAclApply("bob", "group", "readers", false, "consume"));

        assertThat(sm.aclStore().size()).isEqualTo(1);
    }

    @Test
    void snapshotAndRestoreRoundTripsAcls() throws Exception {
        var src = new MetadataStateMachine(new TopicManager());
        src.apply(aclApply("alice", "topic", "orders", false, "produce", true));
        src.apply(aclApply("svc", "topic", "invoices-", true, "*", true));
        src.apply(aclApply("mallory", "cluster", "*", false, "admin", false));

        var baos = new ByteArrayOutputStream();
        src.snapshot(baos);
        var dst = new MetadataStateMachine(new TopicManager());
        dst.restore(new ByteArrayInputStream(baos.toByteArray()));

        assertThat(dst.aclStore().list())
                .containsExactlyInAnyOrderElementsOf(src.aclStore().list());
        assertThat(dst.aclStore().allows("svc", "topic", "invoices-2026", "consume"))
                .isTrue();
        assertThat(dst.aclStore().allows("mallory", "cluster", "cluster", "admin"))
                .isFalse();
    }

    @Test
    void restoreAcceptsV7SnapshotWithoutAclSection() throws Exception {
        // A v7 snapshot ends after the producer-id counter. Build one by
        // hand and assert restore leaves the store empty instead of
        // failing on EOF.
        var baos = new ByteArrayOutputStream();
        var dout = new java.io.DataOutputStream(baos);
        dout.writeByte(7);
        dout.writeInt(0); // topics
        dout.writeInt(0); // partition assignments
        dout.writeLong(0L); // producer-id counter

        var sm = new MetadataStateMachine(new TopicManager());
        sm.restore(new ByteArrayInputStream(baos.toByteArray()));

        assertThat(sm.aclStore().list()).isEmpty();
    }

    private static LogEntry aclApply(
            String principal, String type, String name, boolean prefix, String operation, boolean allow) {
        var record = MetadataRecord.newBuilder()
                .setAcl(AclRecord.newBuilder()
                        .setPrincipal(principal)
                        .setResourceType(type)
                        .setResourceName(name)
                        .setPrefix(prefix)
                        .setOperation(operation)
                        .setAllow(allow))
                .build();
        return new LogEntry(1L, new Term(1L), LogEntry.Type.NORMAL, record.toByteArray());
    }

    private static LogEntry removeAclApply(
            String principal, String type, String name, boolean prefix, String operation) {
        var record = MetadataRecord.newBuilder()
                .setRemoveAcl(RemoveAclRecord.newBuilder()
                        .setPrincipal(principal)
                        .setResourceType(type)
                        .setResourceName(name)
                        .setPrefix(prefix)
                        .setOperation(operation))
                .build();
        return new LogEntry(1L, new Term(1L), LogEntry.Type.NORMAL, record.toByteArray());
    }
}
