package jbroker.broker;

public final class ErrorCodes {
    public static final int NONE = 0;
    public static final int UNKNOWN_TOPIC = 1;
    public static final int INVALID_PARTITION = 2;
    public static final int CORRUPT_BATCH = 3;
    public static final int TOPIC_ALREADY_EXISTS = 4;
    public static final int NOT_LEADER = 5;
    public static final int IO_ERROR = 6;
    public static final int FENCED_EPOCH = 7;
    public static final int OUT_OF_ORDER_SEQUENCE = 8;
    /**
     * acks=all could not be satisfied. Three triggers: the ISR was below
     * {@code min.insync.replicas} before the append (rejected pre-append),
     * the ISR shrank below the floor while waiting, or the HWM did not
     * reach the produced offset within the timeout. Always retriable —
     * pre-append rejections wrote nothing, and after-append cases dedup
     * under an idempotent producer.
     */
    public static final int NOT_ENOUGH_REPLICAS = 9;
    /** Topic config rejected at create/update (e.g. min.insync.replicas above the replication factor). */
    public static final int INVALID_CONFIG = 10;
    /** Produce batch exceeds max.message.bytes. Fatal — retrying the same batch can never succeed. */
    public static final int MESSAGE_TOO_LARGE = 11;
    /**
     * Data volume is below the configured headroom watermark; client
     * produces are refused pre-append while fetch, replication, and admin
     * keep serving. Retriable — the broker recovers on its own once space
     * frees (retention tick, topic deletion, operator action).
     */
    public static final int STORAGE_FULL = 12;

    /**
     * The authenticated principal holds no ACL allowing this operation on
     * this resource (default-deny whenever {@code auth.mode != none}).
     * Fatal — retrying cannot succeed until an operator grants access.
     */
    public static final int UNAUTHORIZED = 13;

    // Consumer groups. Numerics match common.proto::ErrorCode.
    /** Coordinator-routed RPC arrived before {@code __consumer_offsets} has a leader. */
    public static final int COORDINATOR_NOT_AVAILABLE = 80;
    /** This broker is not the coordinator for the requested group; client must {@code FindCoordinator}. */
    public static final int NOT_COORDINATOR = 81;
    /** Heartbeat from an unknown / evicted member; client must rejoin with empty member_id. */
    public static final int UNKNOWN_MEMBER_ID = 82;
    /** Heartbeat carries stale member_epoch; client must rejoin. */
    public static final int FENCED_MEMBER_EPOCH = 83;
    /** {@code FetchOffsets} for a (group, tp) the group has never committed. */
    public static final int OFFSET_OUT_OF_RANGE = 84;
    /** Client-supplied fetch {@code session_id} was not found in the broker cache. */
    public static final int FETCH_SESSION_ID_NOT_FOUND = 85;

    // Rate limiting.
    /** Per-principal quota exceeded on produce or fetch path. Retry after {@code throttleMillis}. */
    public static final int QUOTA_VIOLATED = 86;

    // Admin surface placeholders.
    /** Skeleton placeholder: RPC is wired but the body has not yet been implemented. */
    public static final int UNIMPLEMENTED = 99;

    private ErrorCodes() {}
}
