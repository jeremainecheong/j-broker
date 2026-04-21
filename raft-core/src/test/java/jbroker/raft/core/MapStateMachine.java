package jbroker.raft.core;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Test-only state machine. Payload format: {@code <keyLen:int><key><value>}.
 */
final class MapStateMachine implements StateMachine {

    private final Map<String, String> store = new LinkedHashMap<>();

    @Override
    public void apply(LogEntry entry) {
        if (entry.type() != LogEntry.Type.NORMAL || entry.payload().length == 0) {
            return;
        }
        var buf = ByteBuffer.wrap(entry.payload());
        int keyLen = buf.getInt();
        var keyBytes = new byte[keyLen];
        buf.get(keyBytes);
        var valBytes = new byte[buf.remaining()];
        buf.get(valBytes);
        store.put(new String(keyBytes, StandardCharsets.UTF_8), new String(valBytes, StandardCharsets.UTF_8));
    }

    public Map<String, String> snapshot() {
        return Map.copyOf(store);
    }

    public static byte[] encode(String key, String value) {
        var k = key.getBytes(StandardCharsets.UTF_8);
        var v = value.getBytes(StandardCharsets.UTF_8);
        var buf = ByteBuffer.allocate(Integer.BYTES + k.length + v.length);
        buf.putInt(k.length);
        buf.put(k);
        buf.put(v);
        return buf.array();
    }
}
