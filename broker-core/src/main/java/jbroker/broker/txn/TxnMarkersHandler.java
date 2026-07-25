package jbroker.broker.txn;

import jbroker.broker.ErrorCodes;
import jbroker.proto.common.ErrorCode;
import jbroker.proto.txn.TxnMarkerResult;
import jbroker.proto.txn.WriteTxnMarkersRequest;
import jbroker.proto.txn.WriteTxnMarkersResponse;

/**
 * Server side of the {@code TxnMarkers.WriteTxnMarkers} inter-broker RPC:
 * a remote coordinator fans a decided transaction's markers out to this
 * broker for the partitions it leads. Everything of substance —
 * leadership check, coordinator-epoch fencing, idempotent redelivery,
 * replication wait — lives in {@link TxnMarkerWriter}; this class only
 * walks the request shape and maps result codes onto the shared
 * {@link ErrorCode} wire enum.
 */
public final class TxnMarkersHandler {

    private final TxnMarkerWriter writer;

    public TxnMarkersHandler(TxnMarkerWriter writer) {
        this.writer = writer;
    }

    public WriteTxnMarkersResponse handle(WriteTxnMarkersRequest req) {
        var b = WriteTxnMarkersResponse.newBuilder();
        for (var marker : req.getMarkersList()) {
            for (var tp : marker.getPartitionsList()) {
                int code = writer.append(
                        tp.getTopic(),
                        tp.getPartition(),
                        marker.getProducerId(),
                        marker.getProducerEpoch(),
                        marker.getCommit(),
                        marker.getCoordinatorEpoch());
                b.addResults(TxnMarkerResult.newBuilder()
                        .setTp(tp)
                        .setError(toWire(code))
                        .build());
            }
        }
        return b.build();
    }

    /**
     * Internal codes → shared wire enum. The enum has no NOT_LEADER value
     * (leader routing normally rides the per-service Error message);
     * {@code STALE_LEADER_EPOCH} is the closest replication-vocabulary
     * fit — "the broker you addressed no longer leads this partition" —
     * and, like every non-OK code except INVALID_TXN_STATE, tells the
     * coordinator to re-resolve the leader and retry.
     */
    private static ErrorCode toWire(int code) {
        return switch (code) {
            case ErrorCodes.NONE -> ErrorCode.OK;
            case ErrorCodes.NOT_LEADER -> ErrorCode.STALE_LEADER_EPOCH;
            case ErrorCodes.INVALID_TXN_STATE -> ErrorCode.INVALID_TXN_STATE;
            default -> ErrorCode.UNKNOWN;
        };
    }
}
