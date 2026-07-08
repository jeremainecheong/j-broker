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
