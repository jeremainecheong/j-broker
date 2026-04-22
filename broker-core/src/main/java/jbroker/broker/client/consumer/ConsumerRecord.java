package jbroker.broker.client.consumer;

import jbroker.proto.common.TopicPartition;

/**
 * One record returned to a {@link Consumer} from {@code poll}. Carries the
 * partition, offset, and the deserialized key + value.
 */
public record ConsumerRecord<K, V>(TopicPartition tp, long offset, K key, V value) {}
