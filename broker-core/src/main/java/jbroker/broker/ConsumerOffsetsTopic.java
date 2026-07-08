package jbroker.broker;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Optional;

/**
 * Constants + binary key/value codec for the {@code __consumer_offsets}
 * internal topic.
 *
 * <p>Two record types live in the topic:
 * <ul>
 *   <li><b>Type 1 — offset commit.</b><br>
 *       Key bytes: {@code 0x01 | utf8(group) | utf8(topic) | int32(partition)}.<br>
 *       Value bytes: {@code int64(offset) | int32(leader_epoch) |
 *       utf8(metadata) | int64(commit_ts)}.</li>
 *   <li><b>Type 2 — group metadata.</b><br>
 *       Key bytes: {@code 0x02 | utf8(group)}.<br>
 *       Value bytes: serialized {@link GroupMetadataValue} (proto-encoded
 *       internally). The codec commits only the key +
 *       a placeholder value (encoded as a length-prefixed byte
 *       blob so future shape changes don't require key-codec changes).</li>
 * </ul>
 *
 * <p>UTF-8 strings use {@link DataOutputStream#writeUTF}'s 16-bit length
 * prefix; integers are big-endian. Decoders skip unknown record-type bytes
 * (forward-compat for v3+ transactional offset commits).
 */
public final class ConsumerOffsetsTopic {

    /** Topic name. Compaction marker is propagated through {@code TopicRecord.compact}. */
    public static final String NAME = "__consumer_offsets";

    /** Fixed partition count — changing this invalidates every committed offset's coordinator routing. */
    public static final int PARTITION_COUNT = 50;

    private static final byte TYPE_OFFSET = 0x01;
    private static final byte TYPE_GROUP_METADATA = 0x02;

    private ConsumerOffsetsTopic() {}

    // ---------- Type 1: offset commit ----------

    public record OffsetKey(String group, String topic, int partition) {}

    public record OffsetValue(long offset, int leaderEpoch, String metadata, long commitTimestamp) {}

    public static byte[] keyForOffset(String group, String topic, int partition) {
        try (var baos = new ByteArrayOutputStream();
                var dout = new DataOutputStream(baos)) {
            dout.writeByte(TYPE_OFFSET);
            dout.writeUTF(group);
            dout.writeUTF(topic);
            dout.writeInt(partition);
            return baos.toByteArray();
        } catch (IOException e) {
            // ByteArrayOutputStream never throws.
            throw new AssertionError(e);
        }
    }

    public static byte[] valueForOffset(long offset, int leaderEpoch, String metadata, long commitTimestamp) {
        try (var baos = new ByteArrayOutputStream();
                var dout = new DataOutputStream(baos)) {
            dout.writeLong(offset);
            dout.writeInt(leaderEpoch);
            dout.writeUTF(metadata == null ? "" : metadata);
            dout.writeLong(commitTimestamp);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    public static Optional<OffsetKey> decodeOffsetKey(byte[] bytes) {
        if (bytes == null || bytes.length < 1 || bytes[0] != TYPE_OFFSET) {
            return Optional.empty();
        }
        try (var din = new DataInputStream(new ByteArrayInputStream(bytes, 1, bytes.length - 1))) {
            String group = din.readUTF();
            String topic = din.readUTF();
            int partition = din.readInt();
            return Optional.of(new OffsetKey(group, topic, partition));
        } catch (IOException e) {
            // Truncated / corrupt key. Forward-compat decoders skip such records.
            return Optional.empty();
        }
    }

    public static OffsetValue decodeOffsetValue(byte[] bytes) {
        try (var din = new DataInputStream(new ByteArrayInputStream(bytes))) {
            long offset = din.readLong();
            int leaderEpoch = din.readInt();
            String metadata = din.readUTF();
            long ts = din.readLong();
            return new OffsetValue(offset, leaderEpoch, metadata, ts);
        } catch (IOException e) {
            throw new IllegalArgumentException("malformed offset value: " + e.getMessage(), e);
        }
    }

    // ---------- Type 2: group metadata ----------

    public static byte[] keyForGroupMetadata(String group) {
        try (var baos = new ByteArrayOutputStream();
                var dout = new DataOutputStream(baos)) {
            dout.writeByte(TYPE_GROUP_METADATA);
            dout.writeUTF(group);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    public static Optional<String> decodeGroupMetadataKey(byte[] bytes) {
        if (bytes == null || bytes.length < 1 || bytes[0] != TYPE_GROUP_METADATA) {
            return Optional.empty();
        }
        try (var din = new DataInputStream(new ByteArrayInputStream(bytes, 1, bytes.length - 1))) {
            return Optional.of(din.readUTF());
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    /**
     * Per-group state snapshot persisted on every membership change.
     * Read back by the recovery walk on coordinator activation
     * (broker becomes leader of the group's {@code __consumer_offsets}
     * partition) to rebuild {@link jbroker.broker.group.GroupCoordinator}
     * state.
     *
     * <p>Wire format (all big-endian):
     * <pre>
     *   int32  generation
     *   int32  member_count
     *   for each member:
     *     utf8 member_id              (writeUTF: u16 length + bytes)
     *     utf8 instance_id            ("" for dynamic members)
     *     int32 member_epoch
     *     int32 subscribed_topic_count
     *     utf8[]                      subscribed topic names
     *     int32 assignment_count      (TopicPartition pairs)
     *     for each TP:
     *       utf8 topic
     *       int32 partition
     * </pre>
     */
    public record GroupMetadataValue(int generation, java.util.List<MemberSnapshot> members) {
        public GroupMetadataValue {
            members = java.util.List.copyOf(members);
        }
    }

    public record MemberSnapshot(
            String memberId,
            String instanceId,
            int memberEpoch,
            java.util.List<String> subscribedTopics,
            java.util.List<jbroker.proto.common.TopicPartition> assignment) {
        public MemberSnapshot {
            subscribedTopics = java.util.List.copyOf(subscribedTopics);
            assignment = java.util.List.copyOf(assignment);
        }
    }

    public static byte[] valueForGroupMetadata(GroupMetadataValue v) {
        try (var baos = new ByteArrayOutputStream();
                var dout = new DataOutputStream(baos)) {
            dout.writeInt(v.generation());
            dout.writeInt(v.members().size());
            for (var m : v.members()) {
                dout.writeUTF(m.memberId());
                dout.writeUTF(m.instanceId() == null ? "" : m.instanceId());
                dout.writeInt(m.memberEpoch());
                dout.writeInt(m.subscribedTopics().size());
                for (var t : m.subscribedTopics()) {
                    dout.writeUTF(t);
                }
                dout.writeInt(m.assignment().size());
                for (var tp : m.assignment()) {
                    dout.writeUTF(tp.getTopic());
                    dout.writeInt(tp.getPartition());
                }
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    public static GroupMetadataValue decodeGroupMetadataValue(byte[] bytes) {
        try (var din = new DataInputStream(new ByteArrayInputStream(bytes))) {
            int generation = din.readInt();
            int memberCount = din.readInt();
            var members = new java.util.ArrayList<MemberSnapshot>(memberCount);
            for (int i = 0; i < memberCount; i++) {
                String memberId = din.readUTF();
                String instanceId = din.readUTF();
                int memberEpoch = din.readInt();
                int subCount = din.readInt();
                var subscribed = new java.util.ArrayList<String>(subCount);
                for (int s = 0; s < subCount; s++) subscribed.add(din.readUTF());
                int assignCount = din.readInt();
                var assignment = new java.util.ArrayList<jbroker.proto.common.TopicPartition>(assignCount);
                for (int a = 0; a < assignCount; a++) {
                    String topic = din.readUTF();
                    int partition = din.readInt();
                    assignment.add(jbroker.proto.common.TopicPartition.newBuilder()
                            .setTopic(topic)
                            .setPartition(partition)
                            .build());
                }
                members.add(new MemberSnapshot(memberId, instanceId, memberEpoch, subscribed, assignment));
            }
            return new GroupMetadataValue(generation, members);
        } catch (IOException e) {
            throw new IllegalArgumentException("malformed GroupMetadataValue: " + e.getMessage(), e);
        }
    }
}
