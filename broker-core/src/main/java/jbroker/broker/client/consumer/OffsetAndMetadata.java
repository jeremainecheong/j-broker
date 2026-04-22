package jbroker.broker.client.consumer;

/**
 * Offset to commit for a single partition, with optional client-side
 * metadata and the leader epoch the offset was fetched from.
 *
 * <p>{@code leaderEpoch} of {@code -1} signals "don't track" — the broker
 * accepts that and persists 0. Real applications should pass the
 * {@code leaderEpoch} the consumer last observed when fetching, so a future
 * log-truncation event can be detected on resume.
 */
public record OffsetAndMetadata(long offset, int leaderEpoch, String metadata) {

    public OffsetAndMetadata {
        if (metadata == null) metadata = "";
    }

    public OffsetAndMetadata(long offset) {
        this(offset, -1, "");
    }
}
