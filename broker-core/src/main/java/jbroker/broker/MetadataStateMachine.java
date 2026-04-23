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

    /**
     * P8.3 — fires after a {@code DeleteTopicRecord} applies. Consumers:
     * <ul>
     *   <li>{@code ReplicaFetcherManager.scheduleReconcile()} so in-flight
     *       fetchers on a deleted topic stop promptly rather than waiting
     *       on the next unrelated listener event.</li>
     *   <li>{@code LogManager.deleteTopicDir} so segment files + cached
     *       {@link jbroker.storage.Log} handles do not outlive the topic.
     *       This closes the data-leak window where a topic recreated with
     *       the same name would otherwise see old offsets.</li>
     * </ul>
     */
    @FunctionalInterface
    public interface TopicDeletionListener {
        void onTopicDeleted(String topic);
    }

    private final TopicManager topicManager;
    private final ProducerIdRegistry producerIdRegistry;
    private final LeaderEpochListener leaderEpochListener;
    private final BrokerRegistrationListener brokerRegistrationListener;
    private final PartitionChangeListener partitionChangeListener;
    private final TopicDeletionListener topicDeletionListener;

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
        this(
                topicManager,
                producerIdRegistry,
                leaderEpochListener,
                brokerRegistrationListener,
                partitionChangeListener,
                t -> {});
    }

    public MetadataStateMachine(
            TopicManager topicManager,
            ProducerIdRegistry producerIdRegistry,
            LeaderEpochListener leaderEpochListener,
            BrokerRegistrationListener brokerRegistrationListener,
            PartitionChangeListener partitionChangeListener,
            TopicDeletionListener topicDeletionListener) {
        this.topicManager = topicManager;
        this.producerIdRegistry = producerIdRegistry;
        this.leaderEpochListener = leaderEpochListener;
        this.brokerRegistrationListener = brokerRegistrationListener;
        this.partitionChangeListener = partitionChangeListener;
        this.topicDeletionListener = topicDeletionListener;
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
        // P10.1 — Java 21 pattern-matching switch over the proto oneof's
        // generated KindCase enum. Prefer this over the historical
        // has*() chain: the compiler warns on a non-exhaustive switch so
        // adding a new MetadataRecord variant without an apply branch is
        // a build-time failure rather than a silent drop.
        switch (record.getKindCase()) {
            case TOPIC -> {
                var t = record.getTopic();
                topicManager.onTopicCommitted(
                        t.getTopic(),
                        t.getPartitions(),
                        t.getReplicationFactor(),
                        t.getCreatedMillis(),
                        t.getInternal(),
                        t.getCompact(),
                        t.getConfigMap());
            }
            case PARTITION_CHANGE -> applyPartitionChange(record.getPartitionChange());
            case CREATE_TOPIC -> {
                var ct = record.getCreateTopic();
                var t = ct.getTopic();
                topicManager.onTopicCommitted(
                        t.getTopic(),
                        t.getPartitions(),
                        t.getReplicationFactor(),
                        t.getCreatedMillis(),
                        t.getInternal(),
                        t.getCompact(),
                        t.getConfigMap());
                for (var p : ct.getPartitionChangesList()) {
                    applyPartitionChange(p);
                }
            }
                // P6.7: advance the producer-id counter. ProducerIdRegistry is
                // idempotent under replay — duplicate / out-of-order applies
                // cannot regress the counter.
            case PRODUCER_ID_ASSIGNMENT -> producerIdRegistry.applyAssignment(
                    record.getProducerIdAssignment().getNextProducerId());
            case BROKER -> {
                // P6.5.a: broker-gRPC address discovery.
                var r = record.getBroker();
                brokerRegistrationListener.onBrokerRegistration(r.getBrokerId(), r.getHost(), r.getPort());
            }
            case DELETE_TOPIC -> {
                // P8.3 — drop the topic from the catalogue, then let the
                // broker-level listener evict LogManager cache entries,
                // delete on-disk segments, and kick the replica-fetcher
                // reconcile loop so any in-flight fetch against the now-
                // gone partitions stops promptly.
                var deletedTopic = record.getDeleteTopic().getTopic();
                topicManager.onTopicDeleted(deletedTopic);
                topicDeletionListener.onTopicDeleted(deletedTopic);
            }
            case UPDATE_TOPIC_CONFIG -> {
                var u = record.getUpdateTopicConfig();
                topicManager.onTopicConfigUpdated(u.getTopic(), u.getConfigMap());
            }
                // PartitionRecord is not yet dispatched — unused by any
                // consumer. KIND_NOT_SET is legal (pre-v4 snapshot frames).
            case PARTITION, KIND_NOT_SET -> {
                /* no-op */
            }
        }
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
    //  v7 (P8.3):     per-topic config map.
    private static final byte SNAPSHOT_VERSION = 7;

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
            // v7: config map (length-prefixed key/value pairs).
            var config = t.config();
            dout.writeInt(config.size());
            for (var entry : config.entrySet()) {
                dout.writeUTF(entry.getKey());
                dout.writeUTF(entry.getValue());
            }
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
            java.util.Map<String, String> config;
            if (version >= 7) {
                int cfgSize = din.readInt();
                var cfg = new java.util.HashMap<String, String>(cfgSize);
                for (int j = 0; j < cfgSize; j++) {
                    var k = din.readUTF();
                    var v = din.readUTF();
                    cfg.put(k, v);
                }
                config = cfg;
            } else {
                config = java.util.Map.of();
            }
            topicManager.onTopicCommitted(topic, partitions, rf, created, internal, compact, config);
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
