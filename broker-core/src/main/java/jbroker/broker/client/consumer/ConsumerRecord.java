package jbroker.broker.client.consumer;

import jbroker.proto.common.TopicPartition;
import jbroker.storage.Record;

/**
 * One record returned to a {@link Consumer} from {@code poll}. Carries the
 * partition, offset, deserialized key + value, and the raw header pairs as
 * alternating {@code [k0, v0, k1, v1, ...]} bytes (see
 * {@link Record#headers()} for the wire-side invariant).
 */
public record ConsumerRecord<K, V>(TopicPartition tp, long offset, K key, V value, byte[][] headers) {

    public ConsumerRecord(TopicPartition tp, long offset, K key, V value) {
        this(tp, offset, key, value, Record.NO_HEADERS);
    }
}
