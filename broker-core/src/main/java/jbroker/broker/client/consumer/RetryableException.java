package jbroker.broker.client.consumer;

/**
 * Thrown by a {@link RecordHandler} to signal that the current record
 * should be retried up to the configured {@link DeadLetterPolicy#maxAttempts()}
 * limit before being routed to the dead-letter topic.
 *
 * <p>Any other exception escaping the handler is treated as fatal and
 * propagates out of {@code Consumer.poll} without DLT routing — if a bug
 * corrupts every record you probably don't want 100% of traffic silently
 * re-routed.
 */
public class RetryableException extends Exception {

    private static final long serialVersionUID = 1L;

    public RetryableException(String message) {
        super(message);
    }

    public RetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
