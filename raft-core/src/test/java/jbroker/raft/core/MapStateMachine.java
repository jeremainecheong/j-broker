package jbroker.raft.core;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

    @Override
    public void snapshot(OutputStream out) throws IOException {
        var dout = new DataOutputStream(out);
        dout.writeInt(store.size());
        for (var e : store.entrySet()) {
            dout.writeUTF(e.getKey());
            dout.writeUTF(e.getValue());
        }
        dout.flush();
    }

    @Override
    public void restore(InputStream in) throws IOException {
        store.clear();
        var din = new DataInputStream(in);
        int n = din.readInt();
        for (int i = 0; i < n; i++) {
            var k = din.readUTF();
            var v = din.readUTF();
            store.put(k, v);
        }
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
