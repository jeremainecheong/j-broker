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

    private ErrorCodes() {}
}
