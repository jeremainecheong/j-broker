package jbroker.admin.events;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Redis pub/sub bridge for multi-admin-pod deployments.
 *
 * <p>Each admin pod holds an in-process {@link AdminEventBus} fed by direct
 * gRPC subscriptions to every broker. In a single-pod deployment that's
 * fine; in a load-balanced multi-pod one, admin-originated events (or
 * broker events a pod temporarily lost its stream to) would be visible
 * only to the pod the browser is pinned to. This fanout closes the gap:
 *
 * <ol>
 *   <li>Every locally-ingested event is PUBLISHed to a shared Redis
 *       channel as a compact JSON payload tagged with this pod's id.</li>
 *   <li>A background thread holds a second connection SUBSCRIBEd to the
 *       same channel. Messages whose {@code pod_id} matches self are
 *       skipped (avoid infinite echo); the rest are handed to
 *       {@link AdminEventBus#injectExternal} which de-dupes on
 *       {@code (brokerEndpoint, brokerEventId)} and broadcasts to local
 *       SSE subscribers.</li>
 * </ol>
 *
 * <p>Opt-in via {@code jbroker.redis.url}; when empty the component
 * initialises as a no-op and the bus runs pure in-process. Redis is never
 * the source of truth for events — if the publisher socket errors, the
 * call logs once-per-minute and returns so the SSE pipeline is never
 * blocked by an unreachable Redis.
 *
 * <p>Hand-rolled RESP (pattern mirrors
 * {@link jbroker.broker.quota.RedisQuotaEnforcer}) rather than pulling in
 * Jedis / Lettuce — the functionality we need (PUBLISH + SUBSCRIBE on
 * a single channel) is ~100 lines of protocol handling.
 */
@Component
public class RedisEventFanout {

    private static final Logger log = LoggerFactory.getLogger(RedisEventFanout.class);
    private static final int CONNECT_TIMEOUT_MS = 500;
    private static final int PUBLISH_READ_TIMEOUT_MS = 500;
    private static final String CHANNEL = "jbroker:admin:events";

    private final AdminEventBus bus;
    private final String redisUrl;
    private final String selfPodId = UUID.randomUUID().toString();

    private final ReentrantLock publisherLock = new ReentrantLock();
    private Socket publisherSocket;
    private OutputStream publisherOut;
    private BufferedInputStream publisherIn;

    private volatile Socket subscriberSocket;
    private volatile Thread subscriberThread;
    private volatile boolean running = true;

    public RedisEventFanout(AdminEventBus bus, @Value("${jbroker.redis.url:}") String redisUrl) {
        this.bus = bus;
        this.redisUrl = redisUrl;
    }

    @PostConstruct
    void start() {
        if (redisUrl == null || redisUrl.isBlank()) {
            log.info("jbroker.redis.url not set; Redis admin event fan-out disabled");
            return;
        }
        log.info("Redis admin event fan-out enabled (pod_id={}, channel={})", selfPodId, CHANNEL);
        bus.setExternalPublisher(this::publish);
        subscriberThread = Thread.ofVirtual().name("redis-event-subscriber").start(this::subscriberLoop);
    }

    @PreDestroy
    void stop() {
        running = false;
        bus.setExternalPublisher(null);
        closeQuietly(subscriberSocket);
        publisherLock.lock();
        try {
            closePublisherQuietly();
        } finally {
            publisherLock.unlock();
        }
        if (subscriberThread != null) subscriberThread.interrupt();
    }

    private void publish(AdminEventBus.LocalEvent e) {
        String payload = encode(e);
        publisherLock.lock();
        try {
            if (publisherSocket == null || publisherSocket.isClosed()) {
                connectPublisher();
            }
            try {
                writeArray(publisherOut, new String[] {"PUBLISH", CHANNEL, payload});
                publisherOut.flush();
                // Discard the integer reply (number of subscribers that received).
                readReply(publisherIn);
            } catch (IOException ioe) {
                closePublisherQuietly();
                throw ioe;
            }
        } catch (Exception err) {
            if (publishLogger.tryLog()) {
                log.warn("Redis PUBLISH failed; event {} dropped from fan-out: {}", e.id(), err.toString());
            }
        } finally {
            publisherLock.unlock();
        }
    }

    private void subscriberLoop() {
        while (running) {
            Socket sock = null;
            try {
                var uri = parseRedisUri(redisUrl);
                sock = new Socket();
                sock.setTcpNoDelay(true);
                sock.connect(
                        new InetSocketAddress(uri.getHost(), uri.getPort() > 0 ? uri.getPort() : 6379),
                        CONNECT_TIMEOUT_MS);
                this.subscriberSocket = sock;
                var out = sock.getOutputStream();
                var in = new BufferedInputStream(sock.getInputStream());
                writeArray(out, new String[] {"SUBSCRIBE", CHANNEL});
                out.flush();
                // First reply confirms the subscribe (array of 3:
                // [subscribe, channel, count]). Discard.
                readReply(in);

                while (running) {
                    var decoded = readPubSubMessage(in);
                    if (decoded == null) continue;
                    handleIncoming(decoded);
                }
            } catch (IOException ioe) {
                if (!running) return;
                if (subscribeLogger.tryLog()) {
                    log.warn("Redis SUBSCRIBE disconnected; reconnecting: {}", ioe.toString());
                }
            } finally {
                closeQuietly(sock);
            }
            if (running) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void handleIncoming(String payload) {
        try {
            var parsed = decode(payload);
            if (parsed.podId().equals(selfPodId)) {
                // Our own echo — skip. The local broadcast already happened.
                return;
            }
            bus.injectExternal(parsed.brokerEndpoint(), parsed.type(), parsed.dataJson(), parsed.brokerEventId());
        } catch (Exception e) {
            log.debug("Redis fanout: failed to parse incoming payload: {}", e.toString());
        }
    }

    /** Minimal JSON envelope; tight enough to hand-roll without pulling in Jackson here. */
    record IncomingEvent(String podId, String brokerEndpoint, String type, String dataJson, long brokerEventId) {}

    String encode(AdminEventBus.LocalEvent e) {
        // Outer envelope carries metadata; the broker-emitted {@code data_json}
        // is already a JSON string, so we embed it as a JSON-string literal
        // (escaped) to keep parsing shallow on the receive side.
        var sb = new StringBuilder(128);
        sb.append('{');
        appendStringField(sb, "pod_id", selfPodId);
        sb.append(',');
        appendStringField(sb, "broker_endpoint", e.brokerEndpoint());
        sb.append(',');
        appendStringField(sb, "type", e.type());
        sb.append(',');
        appendStringField(sb, "data_json", e.dataJson());
        sb.append(',');
        sb.append("\"broker_event_id\":").append(e.brokerEventId());
        sb.append('}');
        return sb.toString();
    }

    IncomingEvent decode(String payload) {
        // Tiny hand-rolled parser — the envelope shape is fixed so we don't
        // need a general-purpose JSON library in the hot receive path.
        // Expected keys: pod_id, broker_endpoint, type, data_json, broker_event_id.
        String podId = extractString(payload, "pod_id");
        String brokerEndpoint = extractString(payload, "broker_endpoint");
        String type = extractString(payload, "type");
        String dataJson = extractString(payload, "data_json");
        long brokerEventId = extractLong(payload, "broker_event_id");
        return new IncomingEvent(podId, brokerEndpoint, type, dataJson, brokerEventId);
    }

    // --- RESP / JSON helpers ---

    private void connectPublisher() throws IOException {
        var uri = parseRedisUri(redisUrl);
        var sock = new Socket();
        sock.setTcpNoDelay(true);
        sock.setSoTimeout(PUBLISH_READ_TIMEOUT_MS);
        sock.connect(
                new InetSocketAddress(uri.getHost(), uri.getPort() > 0 ? uri.getPort() : 6379), CONNECT_TIMEOUT_MS);
        this.publisherSocket = sock;
        this.publisherOut = sock.getOutputStream();
        this.publisherIn = new BufferedInputStream(sock.getInputStream());
    }

    private void closePublisherQuietly() {
        closeQuietly(publisherSocket);
        publisherSocket = null;
        publisherOut = null;
        publisherIn = null;
    }

    private static void closeQuietly(Socket s) {
        if (s == null) return;
        try {
            s.close();
        } catch (IOException ignored) {
            // best-effort
        }
    }

    private static URI parseRedisUri(String url) throws IOException {
        try {
            var uri = URI.create(url);
            if (!"redis".equalsIgnoreCase(uri.getScheme())) {
                throw new IOException("unsupported scheme: " + uri.getScheme());
            }
            return uri;
        } catch (IllegalArgumentException iae) {
            throw new IOException("bad redis url: " + url, iae);
        }
    }

    private static void writeArray(OutputStream out, String[] tokens) throws IOException {
        var sb = new StringBuilder(32 + 16 * tokens.length);
        sb.append('*').append(tokens.length).append("\r\n");
        for (var t : tokens) {
            byte[] tb = t.getBytes(StandardCharsets.UTF_8);
            sb.append('$').append(tb.length).append("\r\n").append(t).append("\r\n");
        }
        out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void readReply(InputStream in) throws IOException {
        int prefix = in.read();
        if (prefix < 0) throw new IOException("EOF before reply");
        switch (prefix) {
            case ':', '+' -> readLine(in);
            case '-' -> throw new IOException("redis error: " + readLine(in));
            case '$' -> {
                int len = Integer.parseInt(readLine(in));
                if (len >= 0) {
                    byte[] buf = new byte[len];
                    int off = 0;
                    while (off < len) {
                        int r = in.read(buf, off, len - off);
                        if (r < 0) throw new IOException("unexpected EOF in bulk");
                        off += r;
                    }
                    if (in.read() != '\r' || in.read() != '\n') {
                        throw new IOException("bulk not CRLF-terminated");
                    }
                }
            }
            case '*' -> {
                int n = Integer.parseInt(readLine(in));
                for (int i = 0; i < n; i++) readReply(in);
            }
            default -> throw new IOException("unknown RESP prefix: " + (char) prefix);
        }
    }

    /** Reads a single pub/sub message array; returns the payload bulk string, or null for non-message frames. */
    private static String readPubSubMessage(InputStream in) throws IOException {
        int prefix = in.read();
        if (prefix == -1) throw new IOException("EOF on subscriber socket");
        if (prefix != '*') throw new IOException("unexpected non-array frame on pub/sub: " + (char) prefix);
        int n = Integer.parseInt(readLine(in));
        // Pub/sub "message" frames look like: [message, channel, payload].
        // Subscribe confirmations look like: [subscribe, channel, count].
        String kind = readBulkString(in);
        if (n >= 2) readReply(in); // skip channel name
        if (n >= 3) {
            if ("message".equals(kind)) {
                return readBulkString(in);
            }
            // non-message frame: drain remaining n-2 elements
            for (int i = 2; i < n; i++) readReply(in);
            return null;
        }
        return null;
    }

    private static String readBulkString(InputStream in) throws IOException {
        int prefix = in.read();
        if (prefix != '$') throw new IOException("expected bulk string, got " + (char) prefix);
        int len = Integer.parseInt(readLine(in));
        if (len < 0) return null;
        byte[] buf = new byte[len];
        int off = 0;
        while (off < len) {
            int r = in.read(buf, off, len - off);
            if (r < 0) throw new IOException("EOF in bulk");
            off += r;
        }
        if (in.read() != '\r' || in.read() != '\n') throw new IOException("bulk not CRLF-terminated");
        return new String(buf, StandardCharsets.UTF_8);
    }

    private static String readLine(InputStream in) throws IOException {
        var sb = new StringBuilder(16);
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int n = in.read();
                if (n != '\n') throw new IOException("CR not followed by LF");
                return sb.toString();
            }
            sb.append((char) b);
        }
        throw new IOException("EOF reading line");
    }

    // --- minimal JSON helpers ---

    private static void appendStringField(StringBuilder sb, String key, String value) {
        sb.append('"').append(key).append("\":\"").append(escape(value)).append('"');
    }

    private static String escape(String s) {
        if (s == null) return "";
        var sb = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '"' -> sb.append("\\\"");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        return sb.toString();
    }

    static String extractString(String json, String key) {
        String needle = "\"" + key + "\":\"";
        int start = json.indexOf(needle);
        if (start < 0) return "";
        start += needle.length();
        var sb = new StringBuilder(16);
        for (int i = start; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '\\' && i + 1 < json.length()) {
                char next = json.charAt(++i);
                switch (next) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'u' -> {
                        if (i + 4 < json.length()) {
                            sb.append((char) Integer.parseInt(json.substring(i + 1, i + 5), 16));
                            i += 4;
                        }
                    }
                    default -> sb.append(next);
                }
            } else if (c == '"') {
                return sb.toString();
            } else {
                sb.append(c);
            }
        }
        throw new IllegalArgumentException("unterminated string for key: " + key);
    }

    static long extractLong(String json, String key) {
        String needle = "\"" + key + "\":";
        int start = json.indexOf(needle);
        if (start < 0) return -1L;
        start += needle.length();
        int end = start;
        while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
            end++;
        }
        return Long.parseLong(json.substring(start, end));
    }

    // --- rate-limited logging ---

    private final OnceEveryMinute publishLogger = new OnceEveryMinute();
    private final OnceEveryMinute subscribeLogger = new OnceEveryMinute();

    private static final class OnceEveryMinute {
        private volatile long last;

        synchronized boolean tryLog() {
            long now = System.currentTimeMillis();
            if (now - last > 60_000L) {
                last = now;
                return true;
            }
            return false;
        }
    }

    // --- test accessors ---

    String selfPodId() {
        return selfPodId;
    }

    static String channel() {
        return CHANNEL;
    }
}
