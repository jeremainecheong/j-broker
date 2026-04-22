package jbroker.broker.client.consumer;

/**
 * Callback invoked by {@link Consumer#poll(java.time.Duration, RecordHandler)}
 * for each fetched record. Throw {@link RetryableException} to request a
 * retry-then-DLT disposition per the consumer's {@link DeadLetterPolicy};
 * any other exception fails fast out of {@code poll}.
 */
@FunctionalInterface
public interface RecordHandler<K, V> {

    void handle(ConsumerRecord<K, V> record) throws RetryableException;
}
