package jbroker.broker.quota;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * / Redis-backed token bucket via {@code INCRBY} + {@code EXPIRE}.
 * Keyed on {@code jbroker:quota:{op}:{principal}:{epochSecond}}; the
 * second-granularity bucket is trivially sliding because keys auto-expire.
 *
 * <p>replaces the earlier HTTP stub with a minimal hand-rolled RESP
 * client. Each {@link #check} pipelines {@code INCRBY} + {@code EXPIRE 2}
 * on a pooled single connection guarded by a lock. On any I/O / parse
 * error the connection is torn down and the call falls back to the
 * in-memory enforcer (fail-open — Redis is never the source of truth).
 *
 * <p>Connection pool: a single socket per enforcer instance. Two brokers
 * sharing a Redis instance will each keep one connection — they see the
 * same counters because RESP {@code INCRBY} is atomic server-side.
 *
 * <p>URL formats supported: {@code redis://host:port},
 * {@code redis://host:port/db} (db ignored — we use SELECT 0 implicitly).
 * Any other scheme throws on connect and we fail-open.
 */
public final class RedisQuotaEnforcer implements QuotaEnforcer {

    private static final Logger log = LoggerFactory.getLogger(RedisQuotaEnforcer.class);
    private static final int CONNECT_TIMEOUT_MS = 500;
    private static final int READ_TIMEOUT_MS = 500;
    private static final int BUCKET_TTL_SECONDS = 2;

    private final String redisUrl;
    private final InMemoryQuotaEnforcer local;
    private final long defaultProduceBytesPerSec;
    private final long defaultFetchBytesPerSec;

    private final ReentrantLock connLock = new ReentrantLock();
    private Socket socket;
    private OutputStream out;
    private BufferedInputStream in;

    public RedisQuotaEnforcer(String redisUrl, long defaultProduceBytesPerSec, long defaultFetchBytesPerSec) {
        this.redisUrl = redisUrl;
        this.local = new InMemoryQuotaEnforcer(defaultProduceBytesPerSec, defaultFetchBytesPerSec);
        this.defaultProduceBytesPerSec = defaultProduceBytesPerSec;
        this.defaultFetchBytesPerSec = defaultFetchBytesPerSec;
    }

    @Override
    public Decision check(String principal, Op op, long bytes) {
        long rate = op == Op.PRODUCE ? defaultProduceBytesPerSec : defaultFetchBytesPerSec;
        long second = System.currentTimeMillis() / 1000L;
        String key = "jbroker:quota:" + op.name().toLowerCase() + ":" + principal + ":" + second;
        try {
            long total = incrByWithExpire(key, bytes);
            if (total > rate) {
                long throttleMs = Math.max(100L, 1000L - (System.currentTimeMillis() % 1000L));
                return Decision.denied(rate, throttleMs);
            }
            return Decision.allowed();
        } catch (Exception e) {
            if (logOnce.tryLog()) {
                log.warn("quota Redis unreachable; falling back to in-memory: {}", e.toString());
            }
            return local.check(principal, op, bytes);
        }
    }

    /**
     * Pipelined {@code INCRBY key bytes} + {@code EXPIRE key 2}. Holds the
     * connection lock for the duration — concurrent callers queue. One
     * persistent TCP socket per enforcer; lazy-reconnect on failure.
     * Returns the post-increment counter.
     */
    long incrByWithExpire(String key, long bytes) throws IOException {
        connLock.lock();
        try {
            if (socket == null || socket.isClosed() || !socket.isConnected()) {
                connect();
            }
            try {
                writeArray(out, new String[] {"INCRBY", key, Long.toString(bytes)});
                writeArray(out, new String[] {"EXPIRE", key, Integer.toString(BUCKET_TTL_SECONDS)});
                out.flush();
                long incremented = readInteger(in);
                // Discard the EXPIRE reply (always integer — 1 if key existed,
                // 0 if it was gone between INCRBY and EXPIRE, which is fine).
                readReply(in);
                return incremented;
            } catch (IOException ioe) {
                // Tear down a bad connection so the next call reconnects.
                closeQuietly();
                throw ioe;
            }
        } finally {
            connLock.unlock();
        }
    }

    private void connect() throws IOException {
        if (redisUrl == null || redisUrl.isBlank()) {
            throw new IOException("no redis url configured");
        }
        URI uri = parseRedisUri(redisUrl);
        var sock = new Socket();
        sock.setTcpNoDelay(true);
        sock.setSoTimeout(READ_TIMEOUT_MS);
        sock.connect(
                new InetSocketAddress(uri.getHost(), uri.getPort() > 0 ? uri.getPort() : 6379), CONNECT_TIMEOUT_MS);
        this.socket = sock;
        this.out = sock.getOutputStream();
        this.in = new BufferedInputStream(sock.getInputStream());
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
        // RESP wire: *<n>\r\n ($<len>\r\n<token>\r\n)+
        var sb = new StringBuilder(32 + 16 * tokens.length);
        sb.append('*').append(tokens.length).append("\r\n");
        for (var t : tokens) {
            byte[] tb = t.getBytes(StandardCharsets.UTF_8);
            sb.append('$').append(tb.length).append("\r\n").append(t).append("\r\n");
        }
        out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static long readInteger(InputStream in) throws IOException {
        int prefix = in.read();
        if (prefix == ':') {
            return Long.parseLong(readLine(in));
        }
        if (prefix == '-') {
            throw new IOException("redis error: " + readLine(in));
        }
        throw new IOException("expected integer reply, got prefix: " + (char) prefix);
    }

    /** Read-and-discard a single RESP reply (any type). */
    private static void readReply(InputStream in) throws IOException {
        int prefix = in.read();
        switch (prefix) {
            case ':' -> readLine(in);
            case '+' -> readLine(in);
            case '-' -> throw new IOException("redis error: " + readLine(in));
            case '$' -> {
                // Bulk string: $<len>\r\n<bytes>\r\n (or $-1\r\n for nil)
                int len = Integer.parseInt(readLine(in));
                if (len >= 0) {
                    byte[] buf = new byte[len];
                    int off = 0;
                    while (off < len) {
                        int r = in.read(buf, off, len - off);
                        if (r < 0) throw new IOException("unexpected EOF in bulk string");
                        off += r;
                    }
                    // Skip the trailing \r\n.
                    if (in.read() != '\r' || in.read() != '\n') {
                        throw new IOException("bulk string not terminated by CRLF");
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
        throw new IOException("unexpected EOF reading line");
    }

    private void closeQuietly() {
        try {
            if (socket != null) socket.close();
        } catch (IOException ignored) {
            // best-effort
        } finally {
            socket = null;
            out = null;
            in = null;
        }
    }

    /** For tests and shutdown hooks. */
    public void close() {
        connLock.lock();
        try {
            closeQuietly();
        } finally {
            connLock.unlock();
        }
    }

    private final OncePerMinuteLogger logOnce = new OncePerMinuteLogger();

    private static final class OncePerMinuteLogger {
        private volatile long lastLogMillis;

        synchronized boolean tryLog() {
            long now = System.currentTimeMillis();
            if (now - lastLogMillis > 60_000L) {
                lastLogMillis = now;
                return true;
            }
            return false;
        }
    }
}
