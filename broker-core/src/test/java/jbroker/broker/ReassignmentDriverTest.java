package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import jbroker.proto.raft.MetadataRecord;
import org.junit.jupiter.api.Test;

class ReassignmentDriverTest {

    private static MetadataRecord parse(byte[] b) throws Exception {
        return MetadataRecord.parseFrom(b);
    }

    @Test
    void proposesTheExpandForAPendingReassignment() throws Exception {
        var topics = new TopicManager();
        topics.onTopicCommitted("t", 1, 2, 1L);
        topics.onPartitionChange("t", 0, 1, List.of(1, 2), List.of(1, 2), 3, 2);
        var store = new ReassignmentStore();
        store.put(new ReassignmentStore.Pending("t", 0, List.of(2, 3), List.of(1, 2)));

        var driver = new ReassignmentDriver(topics, store);
        var proposals = driver.decide();

        assertThat(proposals).hasSize(1);
        var change = parse(proposals.get(0)).getPartitionChange();
        assertThat(change.getReplicasList()).containsExactlyInAnyOrder(1, 2, 3);
    }

    @Test
    void skipsPartitionsWithNoKnownStateYet() {
        var topics = new TopicManager(); // no partition state
        var store = new ReassignmentStore();
        store.put(new ReassignmentStore.Pending("t", 0, List.of(2, 3), List.of(1, 2)));

        assertThat(new ReassignmentDriver(topics, store).decide()).isEmpty();
    }

    @Test
    void emitsNoProposalWhileNewcomersAreCatchingUp() {
        var topics = new TopicManager();
        topics.onTopicCommitted("t", 1, 2, 1L);
        // Already expanded to the union, but 3 is not yet in the ISR.
        topics.onPartitionChange("t", 0, 1, List.of(1, 2), List.of(1, 2, 3), 3, 3);
        var store = new ReassignmentStore();
        store.put(new ReassignmentStore.Pending("t", 0, List.of(2, 3), List.of(1, 2)));

        assertThat(new ReassignmentDriver(topics, store).decide()).isEmpty();
    }

    @Test
    void drivesEachPendingPartitionIndependently() throws Exception {
        var topics = new TopicManager();
        topics.onTopicCommitted("t", 2, 2, 1L);
        topics.onPartitionChange("t", 0, 1, List.of(1, 2), List.of(1, 2), 3, 2);
        topics.onPartitionChange("t", 1, 2, List.of(2, 3), List.of(2, 3), 3, 2);
        var store = new ReassignmentStore();
        store.put(new ReassignmentStore.Pending("t", 0, List.of(2, 3), List.of(1, 2)));
        store.put(new ReassignmentStore.Pending("t", 1, List.of(3, 4), List.of(2, 3)));

        assertThat(new ReassignmentDriver(topics, store).decide()).hasSize(2);
    }
}
