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

    // Cluster lifecycle.
    /**
     * A membership change or reassignment is already in flight for the
     * target — the controller runs one at a time. Retriable: try again
     * once the in-flight operation completes.
     */
    public static final int REASSIGNMENT_IN_PROGRESS = 87;

    // Transactions. Numerics match common.proto::ErrorCode.
    /**
     * Request carried a (producer_id, producer_epoch) older than the
     * coordinator's current grant for the transactional_id — a newer
     * {@code InitTransactions} fenced this producer. Fatal for the fenced
     * instance; only a fresh {@code InitTransactions} can continue.
     */
    public static final int PRODUCER_FENCED = 88;

    /**
     * Request is not legal in the transaction's current state (e.g.
     * {@code EndTxn} with no open transaction, or commit while an abort is
     * already prepared). Fatal — indicates a client protocol violation.
     */
    public static final int INVALID_TXN_STATE = 89;

    /**
     * The previous transaction for this transactional_id is still
     * completing (markers in flight). Retriable — back off and retry once
     * the completion lands.
     */
    public static final int CONCURRENT_TRANSACTIONS = 90;

    // Admin surface placeholders.
    /** Skeleton placeholder: RPC is wired but the body has not yet been implemented. */
    public static final int UNIMPLEMENTED = 99;

    private ErrorCodes() {}
}
