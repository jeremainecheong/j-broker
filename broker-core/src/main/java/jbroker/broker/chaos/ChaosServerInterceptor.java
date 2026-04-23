package jbroker.broker.chaos;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * P9.3 — inbound chaos gate. Rejects RPCs with {@link Status#UNAVAILABLE}
 * when the broker is paused or the calling peer (decoded from
 * {@code jbroker-from-broker-id} metadata) is blocked. Applies the
 * configured latency-injection delay before delegating when neither
 * gate fires.
 *
 * <p>Ignores RPCs missing the metadata header — client-facing
 * Produce / Fetch come without it, which is the right behaviour: chaos
 * controls broker-to-broker interaction, not the data plane to clients.
 */
public final class ChaosServerInterceptor implements ServerInterceptor {

    private final ChaosState state;

    public ChaosServerInterceptor(ChaosState state) {
        this.state = state;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        if (state.isPaused()) {
            call.close(Status.UNAVAILABLE.withDescription("jbroker-chaos: broker paused"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
        String fromId = headers.get(ChaosMetadataKeys.FROM_BROKER_ID);
        if (fromId != null) {
            try {
                int peer = Integer.parseInt(fromId);
                if (state.isBlocked(peer)) {
                    call.close(
                            Status.UNAVAILABLE.withDescription(
                                    "jbroker-chaos: inbound from broker " + peer + " blocked"),
                            new Metadata());
                    return new ServerCall.Listener<>() {};
                }
            } catch (NumberFormatException ignored) {
                // malformed header → treat as no peer id; pass through.
            }
        }
        state.maybeSleep();
        return next.startCall(call, headers);
    }
}
