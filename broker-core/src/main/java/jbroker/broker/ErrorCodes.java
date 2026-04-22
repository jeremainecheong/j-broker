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
    /** P6.6 acks=all: HWM did not reach the produced offset within the timeout. */
    public static final int NOT_ENOUGH_REPLICAS = 9;

    // Phase 7 — consumer groups. Numerics match common.proto::ErrorCode.
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
    /** P7.11 — client-supplied fetch {@code session_id} was not found in the broker cache. */
    public static final int FETCH_SESSION_ID_NOT_FOUND = 85;

    private ErrorCodes() {}
}
