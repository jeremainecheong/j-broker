package jbroker.broker.client.consumer;

import java.time.Duration;

/**
 * Dead-letter-topic routing policy attached to a {@link Consumer} via
 * {@link ConsumerConfig.Builder#deadLetterPolicy}.
 *
 * <p>When a {@link RecordHandler} rejects a record with
 * {@link RetryableException}, the consumer retries up to {@link #maxAttempts}
 * (sleeping {@link #backoff} between tries — constant; no jitter, no
 * exponential growth in this slice). On final failure the record is produced
 * verbatim to {@link #dltTopic}, decorated with an
 * {@code X-DLT-Failure-Cause} header carrying the last exception's class
 * and message as UTF-8 bytes. The consumer advances its fetch offset and
 * commits past the failed record only after the DLT produce confirms.
 *
 * <p>The DLT topic is <em>not</em> auto-created — callers pre-create it via
 * the admin surface. Records are produced to the same partition number as
 * the source, so the source topic and DLT topic must have the same partition
 * count (or the DLT must have at least as many — excess source partitions
 * will fail the produce with {@code NOT_LEADER}).
 */
public record DeadLetterPolicy(String dltTopic, int maxAttempts, Duration backoff) {

    public DeadLetterPolicy {
        if (dltTopic == null || dltTopic.isBlank()) {
            throw new IllegalArgumentException("dltTopic must be non-blank");
        }
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1, got " + maxAttempts);
        }
        if (backoff == null || backoff.isNegative()) {
            throw new IllegalArgumentException("backoff must be non-negative");
        }
    }
}
