package jbroker.bench;

import jbroker.broker.client.BrokerClient;

/**
 * Topology labels for a CSV row, resolved from the broker's own topic
 * metadata rather than trusted from flags. {@code minInsyncReplicas} is
 * only the EXPLICIT per-topic override — when absent the broker applies
 * its cluster default, which this client cannot observe, so the cell
 * stays empty rather than guessing.
 */
record TopicLabels(Integer partitions, Integer replicationFactor, Integer minInsyncReplicas) {

    static final TopicLabels UNKNOWN = new TopicLabels(null, null, null);

    static TopicLabels resolve(BrokerClient client, String topic) {
        try {
            var d = client.describeTopic(topic);
            Integer minIsr = null;
            var raw = d.getConfigMap().get("min.insync.replicas");
            if (raw != null) {
                try {
                    minIsr = Integer.parseInt(raw.trim());
                } catch (NumberFormatException e) {
                    // Malformed override — leave the cell empty.
                }
            }
            return new TopicLabels(d.getPartitions(), d.getReplicationFactor(), minIsr);
        } catch (RuntimeException e) {
            // Labels are best-effort; a metadata failure must not kill the run.
            return UNKNOWN;
        }
    }
}
