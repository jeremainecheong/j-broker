package jbroker.raft.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Payload codec for {@link LogEntry.Type#CONFIG_CHANGE} entries: a voter list
 * serialised as {@code <count:int32><nodeId:int32>*count}.
 *
 * <p>Kept deliberately minimal — voter lists are tiny and the wire format
 * doesn't need to evolve alongside the normal domain payload.
 */
public final class MembershipCodec {

    private MembershipCodec() {}

    public static byte[] encode(List<NodeId> voters) {
        Objects.requireNonNull(voters, "voters");
        var buf = ByteBuffer.allocate(4 + voters.size() * 4);
        buf.putInt(voters.size());
        for (var v : voters) {
            buf.putInt(v.value());
        }
        return buf.array();
    }

    public static List<NodeId> decode(byte[] payload) {
        Objects.requireNonNull(payload, "payload");
        var buf = ByteBuffer.wrap(payload);
        int n = buf.getInt();
        if (n < 0 || n * 4 + 4 != payload.length) {
            throw new IllegalArgumentException("corrupt CONFIG_CHANGE payload");
        }
        var out = new ArrayList<NodeId>(n);
        for (int i = 0; i < n; i++) {
            out.add(new NodeId(buf.getInt()));
        }
        return List.copyOf(out);
    }
}
