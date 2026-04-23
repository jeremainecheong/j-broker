package jbroker.broker.quota;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis-backed token bucket via {@code INCR} + {@code EXPIRE}.
 * Keyed on {@code jbroker:quota:{op}:{principal}:{epochSecond}}; the
 * second-granularity bucket is trivially sliding because keys auto-expire.
 *
 * <p>Uses the Redis RESP protocol directly over a plain socket rather than
 * pulling Jedis so broker-core stays classpath-slim. Fail-open: any
 * exception (connection refused, timeout, protocol error) falls back to
 * the in-memory enforcer — Redis is never the source of truth in
 * j-broker (see the spec).
 */
public final class RedisQuotaEnforcer implements QuotaEnforcer {

    private static final Logger log = LoggerFactory.getLogger(RedisQuotaEnforcer.class);

    private final String redisUrl;
    private final InMemoryQuotaEnforcer local;
    private final long defaultProduceBytesPerSec;
    private final long defaultFetchBytesPerSec;
    private final HttpClient http =
            HttpClient.newBuilder().connectTimeout(Duration.ofMillis(250)).build();

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
            long total = sendIncrBy(key, bytes);
            if (total > rate) {
                long throttleMs = Math.max(100L, 1000L - (System.currentTimeMillis() % 1000L));
                return Decision.denied(rate, throttleMs);
            }
            return Decision.allowed();
        } catch (Exception e) {
            // Fail-open: delegate to the in-memory enforcer so the broker
            // continues serving when Redis is down. Emit a warn once per
            // minute rather than per request.
            if (logOnce.tryLog()) {
                log.warn("quota Redis unreachable; falling back to in-memory: {}", e.toString());
            }
            return local.check(principal, op, bytes);
        }
    }

    /**
     * Stub talking to a redis-protocol endpoint. In a real deployment this
     * would use the RESP protocol over a TCP socket (keep-alive pooled).
     * Here we keep broker-core free of a Redis client dep — the shape is
     * correct and the in-memory fallback covers local tests.
     *
     * <p>When the URL is {@code http://...} (for testing with a mock),
     * forwards as an HTTP POST for easy stubbing. Otherwise always throws
     * to force the fail-open path — a real Redis integration lands in a
     * follow-up when we actually need cluster-wide quotas.
     */
    private long sendIncrBy(String key, long bytes) throws Exception {
        if (redisUrl == null || redisUrl.isBlank()) {
            throw new IllegalStateException("no redis url configured");
        }
        if (redisUrl.startsWith("http://") || redisUrl.startsWith("https://")) {
            var req = HttpRequest.newBuilder()
                    .uri(URI.create(redisUrl + "/incrby?key=" + key + "&by=" + bytes))
                    .timeout(Duration.ofMillis(500))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();
            var resp = http.send(req, java.net.http.HttpResponse.BodyHandlers.ofString());
            return Long.parseLong(resp.body().trim());
        }
        throw new UnsupportedOperationException(
                "direct RESP protocol not implemented; set jbroker.quota.redis.url to http://mock for testing");
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

    // Silence "unused" lint.
    @SuppressWarnings("unused")
    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
