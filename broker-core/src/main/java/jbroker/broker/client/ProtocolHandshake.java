package jbroker.broker.client;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.function.Supplier;
import jbroker.broker.ProtocolVersion;
import jbroker.proto.broker.ApiVersionsResponse;
import jbroker.proto.common.ErrorCode;

/**
 * The protocol-version handshake shared by {@link ClusterClient} and
 * {@link BrokerClient}: run {@code Metadata.ApiVersions} once per
 * connection before the first real RPC and reject incompatibility loudly.
 *
 * <p>Incompatibility — a range disjoint from this client's, or gRPC
 * {@code UNIMPLEMENTED} from a broker predating the ApiVersions RPC —
 * raises {@link UnsupportedBrokerException} and must never be retried: a
 * version mismatch cannot heal on its own, and routing around it would
 * only mask a broker the operator must upgrade. Transport-level failures
 * (UNAVAILABLE, DEADLINE_EXCEEDED, ...) propagate untouched for the
 * caller's usual connection handling.
 */
final class ProtocolHandshake {

    private ProtocolHandshake() {}

    /**
     * Runs {@code apiVersions} and verifies the advertised range overlaps
     * {@code [ProtocolVersion.MIN_SUPPORTED, ProtocolVersion.CURRENT]}.
     * {@code endpoint} ({@code host:port}) only labels the error messages.
     */
    static void verify(String endpoint, Supplier<ApiVersionsResponse> apiVersions) {
        ApiVersionsResponse resp;
        try {
            resp = apiVersions.get();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.UNIMPLEMENTED) {
                throw UnsupportedBrokerException.missingHandshake(
                        endpoint, ProtocolVersion.MIN_SUPPORTED, ProtocolVersion.CURRENT);
            }
            throw e;
        }
        if (resp.getError() != ErrorCode.OK) {
            throw new IllegalStateException("apiVersions failed against " + endpoint + ": " + resp.getError());
        }
        if (resp.getMinProtocolVersion() > ProtocolVersion.CURRENT
                || resp.getMaxProtocolVersion() < ProtocolVersion.MIN_SUPPORTED) {
            throw UnsupportedBrokerException.incompatibleRange(
                    endpoint,
                    resp.getMinProtocolVersion(),
                    resp.getMaxProtocolVersion(),
                    ProtocolVersion.MIN_SUPPORTED,
                    ProtocolVersion.CURRENT);
        }
    }
}
