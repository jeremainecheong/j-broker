package jbroker.broker.txn;

/**
 * Lifecycle states of one transactional_id's current transaction, as
 * persisted in {@code __transaction_state}. Transitions (all driven by
 * {@link TxnCoordinator} inputs):
 *
 * <pre>
 *   EMPTY ──AddPartitionsToTxn──▶ ONGOING
 *   ONGOING ──AddPartitionsToTxn──▶ ONGOING            (extends the set)
 *   ONGOING ──EndTxn(commit)──▶ PREPARE_COMMIT
 *   ONGOING ──EndTxn(abort) / tick timeout / InitTransactions──▶ PREPARE_ABORT
 *   PREPARE_COMMIT ──MarkersDelivered──▶ COMPLETE_COMMIT
 *   PREPARE_ABORT  ──MarkersDelivered──▶ COMPLETE_ABORT
 *   COMPLETE_* ──AddPartitionsToTxn──▶ ONGOING          (next transaction)
 *   any of EMPTY / COMPLETE_* ──InitTransactions──▶ EMPTY (epoch bump)
 * </pre>
 *
 * <p>Wire codes are explicit (not enum ordinals) so reordering or adding
 * constants can never silently change persisted bytes.
 */
public enum TxnState {
    EMPTY((byte) 0),
    ONGOING((byte) 1),
    PREPARE_COMMIT((byte) 2),
    PREPARE_ABORT((byte) 3),
    COMPLETE_COMMIT((byte) 4),
    COMPLETE_ABORT((byte) 5);

    private final byte wireCode;

    TxnState(byte wireCode) {
        this.wireCode = wireCode;
    }

    /** Stable byte persisted in {@code __transaction_state} values. */
    public byte wireCode() {
        return wireCode;
    }

    /** True for the two decided-but-not-yet-completed states. */
    public boolean isPrepare() {
        return this == PREPARE_COMMIT || this == PREPARE_ABORT;
    }

    /** True once markers are fully delivered and the outcome is closed. */
    public boolean isComplete() {
        return this == COMPLETE_COMMIT || this == COMPLETE_ABORT;
    }

    /** Inverse of {@link #wireCode()}. */
    public static TxnState fromWireCode(byte code) {
        for (var s : values()) {
            if (s.wireCode == code) return s;
        }
        throw new IllegalArgumentException("unknown TxnState wire code: " + code);
    }
}
