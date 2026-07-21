package jbroker.raft.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Payload codec for {@link LogEntry.Type#CONFIG_CHANGE} entries.
 *
 * <p>Two on-disk shapes decode transparently:
 *
 * <ul>
 *   <li><b>Legacy</b> ({@code <count:int32><nodeId:int32>*count}) — a bare
 *       voter list, written before learners existed. {@code count} is
 *       non-negative, which is how it's told apart from the versioned form.
 *   <li><b>Versioned</b> ({@code <-1><version:int32><voters><learners>}) —
 *       leads with the {@code -1} sentinel (never a valid count), then a
 *       version, then a voter list and a learner list, each length-prefixed.
 * </ul>
 *
 * <p>New writes always use the versioned form. Voter lists are tiny, so the
 * format stays deliberately minimal.
 */
public final class MembershipCodec {

    private static final int VERSIONED_SENTINEL = -1;
    private static final int VERSION = 2;

    private MembershipCodec() {}

    /** Encode voters only (no learners) — kept for call sites that don't use learners. */
    public static byte[] encode(List<NodeId> voters) {
        return encode(Membership.ofVoters(voters));
    }

    public static byte[] encode(Membership membership) {
        Objects.requireNonNull(membership, "membership");
        var voters = membership.voters();
        var learners = membership.learners();
        var buf = ByteBuffer.allocate(4 + 4 + 4 + voters.size() * 4 + 4 + learners.size() * 4);
        buf.putInt(VERSIONED_SENTINEL);
        buf.putInt(VERSION);
        buf.putInt(voters.size());
        for (var v : voters) {
            buf.putInt(v.value());
        }
        buf.putInt(learners.size());
        for (var l : learners) {
            buf.putInt(l.value());
        }
        return buf.array();
    }

    /** The voter list only. Legacy and versioned payloads both supported. */
    public static List<NodeId> decode(byte[] payload) {
        return decodeMembership(payload).voters();
    }

    public static Membership decodeMembership(byte[] payload) {
        Objects.requireNonNull(payload, "payload");
        if (payload.length < 4) {
            throw new IllegalArgumentException("corrupt CONFIG_CHANGE payload");
        }
        var buf = ByteBuffer.wrap(payload);
        int first = buf.getInt();
        if (first == VERSIONED_SENTINEL) {
            int version = buf.getInt();
            if (version != VERSION) {
                throw new IllegalArgumentException("unsupported CONFIG_CHANGE version: " + version);
            }
            var voters = readList(buf, payload);
            var learners = readList(buf, payload);
            if (buf.hasRemaining()) {
                throw new IllegalArgumentException("corrupt CONFIG_CHANGE payload: trailing bytes");
            }
            return new Membership(voters, learners);
        }
        // Legacy: `first` was the voter count.
        if (first < 0 || first * 4 + 4 != payload.length) {
            throw new IllegalArgumentException("corrupt CONFIG_CHANGE payload");
        }
        var voters = new ArrayList<NodeId>(first);
        for (int i = 0; i < first; i++) {
            voters.add(new NodeId(buf.getInt()));
        }
        return Membership.ofVoters(voters);
    }

    private static List<NodeId> readList(ByteBuffer buf, byte[] payload) {
        if (buf.remaining() < 4) {
            throw new IllegalArgumentException("corrupt CONFIG_CHANGE payload");
        }
        int n = buf.getInt();
        if (n < 0 || buf.remaining() < n * 4) {
            throw new IllegalArgumentException("corrupt CONFIG_CHANGE payload");
        }
        var out = new ArrayList<NodeId>(n);
        for (int i = 0; i < n; i++) {
            out.add(new NodeId(buf.getInt()));
        }
        return out;
    }
}
