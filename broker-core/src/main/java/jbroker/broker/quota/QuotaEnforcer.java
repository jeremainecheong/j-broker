package jbroker.broker.quota;

/**
 * Byte-bucket rate limiter for produce / fetch paths. Implementations
 * may be Redis-backed ({@link RedisQuotaEnforcer}), in-memory, or a no-op.
 *
 * <p>{@link #check} is called with the number of bytes the caller
 * <em>wants</em> to spend; the enforcer atomically admits or denies.
 * Denied calls get a hint of how long to back off before retrying —
 * clients can surface this as a {@code retry-after}-style header.
 */
public interface QuotaEnforcer {

    QuotaEnforcer NOOP = (principal, op, bytes) -> Decision.allowed();

    Decision check(String principal, Op op, long bytes);

    /**
     * The enforcer a broker's quota settings call for. A rate of zero (or
     * less) disables that op's quota entirely — its checks always admit,
     * exactly as {@link #NOOP} would; with both rates disabled the result
     * IS {@link #NOOP}. A non-blank {@code redisUrl} selects the
     * Redis-backed enforcer so per-principal caps hold cluster-wide;
     * blank or null keeps enforcement in-process (the same convention the
     * admin event fan-out uses for its Redis URL).
     */
    static QuotaEnforcer configured(long produceBytesPerSec, long fetchBytesPerSec, String redisUrl) {
        boolean produceOn = produceBytesPerSec > 0;
        boolean fetchOn = fetchBytesPerSec > 0;
        if (!produceOn && !fetchOn) return NOOP;
        QuotaEnforcer delegate = redisUrl != null && !redisUrl.isBlank()
                ? new RedisQuotaEnforcer(redisUrl, produceBytesPerSec, fetchBytesPerSec)
                : new InMemoryQuotaEnforcer(produceBytesPerSec, fetchBytesPerSec);
        if (produceOn && fetchOn) return delegate;
        // One op has no quota configured: it must stay unlimited, not
        // inherit the other op's rate, so its checks never reach the
        // delegate (whose constructors clamp a zero rate up to 1 B/s).
        return (principal, op, bytes) -> {
            boolean enabled = op == Op.PRODUCE ? produceOn : fetchOn;
            return enabled ? delegate.check(principal, op, bytes) : Decision.allowed();
        };
    }

    enum Op {
        PRODUCE,
        FETCH
    }

    record Decision(boolean allow, long quotaBytesPerSec, long throttleMillis) {
        public static Decision allowed() {
            return new Decision(true, Long.MAX_VALUE, 0L);
        }

        public static Decision denied(long quotaBytesPerSec, long throttleMillis) {
            return new Decision(false, quotaBytesPerSec, throttleMillis);
        }
    }
}
