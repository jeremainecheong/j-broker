package jbroker.broker;

import java.util.Map;

/**
 * Maps numeric broker error codes (see {@link ErrorCodes}) to their
 * string-form names. The admin REST layer uses the names as the
 * {@code error_code} field of the JSON error envelope defined in
 * E.g. numeric {@code 5} renders as {@code "NOT_LEADER"}.
 *
 * <p>Every public {@code int} constant in {@link ErrorCodes} has a
 * corresponding entry here; {@link #nameTable()} is read-only and safe to
 * share across threads.
 */
public final class ErrorCodeNames {

    private static final Map<Integer, String> NAMES = Map.ofEntries(
            Map.entry(ErrorCodes.NONE, "NONE"),
            Map.entry(ErrorCodes.UNKNOWN_TOPIC, "UNKNOWN_TOPIC"),
            Map.entry(ErrorCodes.INVALID_PARTITION, "INVALID_PARTITION"),
            Map.entry(ErrorCodes.CORRUPT_BATCH, "CORRUPT_BATCH"),
            Map.entry(ErrorCodes.TOPIC_ALREADY_EXISTS, "TOPIC_ALREADY_EXISTS"),
            Map.entry(ErrorCodes.NOT_LEADER, "NOT_LEADER"),
            Map.entry(ErrorCodes.IO_ERROR, "IO_ERROR"),
            Map.entry(ErrorCodes.FENCED_EPOCH, "FENCED_EPOCH"),
            Map.entry(ErrorCodes.OUT_OF_ORDER_SEQUENCE, "OUT_OF_ORDER_SEQUENCE"),
            Map.entry(ErrorCodes.NOT_ENOUGH_REPLICAS, "NOT_ENOUGH_REPLICAS"),
            Map.entry(ErrorCodes.INVALID_CONFIG, "INVALID_CONFIG"),
            Map.entry(ErrorCodes.MESSAGE_TOO_LARGE, "MESSAGE_TOO_LARGE"),
            Map.entry(ErrorCodes.STORAGE_FULL, "STORAGE_FULL"),
            Map.entry(ErrorCodes.UNAUTHORIZED, "UNAUTHORIZED"),
            Map.entry(ErrorCodes.COORDINATOR_NOT_AVAILABLE, "COORDINATOR_NOT_AVAILABLE"),
            Map.entry(ErrorCodes.NOT_COORDINATOR, "NOT_COORDINATOR"),
            Map.entry(ErrorCodes.UNKNOWN_MEMBER_ID, "UNKNOWN_MEMBER_ID"),
            Map.entry(ErrorCodes.FENCED_MEMBER_EPOCH, "FENCED_MEMBER_EPOCH"),
            Map.entry(ErrorCodes.OFFSET_OUT_OF_RANGE, "OFFSET_OUT_OF_RANGE"),
            Map.entry(ErrorCodes.FETCH_SESSION_ID_NOT_FOUND, "FETCH_SESSION_ID_NOT_FOUND"),
            Map.entry(ErrorCodes.QUOTA_VIOLATED, "QUOTA_VIOLATED"),
            Map.entry(ErrorCodes.REASSIGNMENT_IN_PROGRESS, "REASSIGNMENT_IN_PROGRESS"),
            Map.entry(ErrorCodes.PRODUCER_FENCED, "PRODUCER_FENCED"),
            Map.entry(ErrorCodes.INVALID_TXN_STATE, "INVALID_TXN_STATE"),
            Map.entry(ErrorCodes.CONCURRENT_TRANSACTIONS, "CONCURRENT_TRANSACTIONS"),
            Map.entry(ErrorCodes.UNIMPLEMENTED, "UNIMPLEMENTED"));

    /** Returns the canonical name for {@code code}, or {@code "UNKNOWN"} if unrecognised. */
    public static String name(int code) {
        return NAMES.getOrDefault(code, "UNKNOWN");
    }

    /** Read-only view of the full code → name table. */
    public static Map<Integer, String> nameTable() {
        return NAMES;
    }

    private ErrorCodeNames() {}
}
