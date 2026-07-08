package jbroker.broker.chaos;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Outbound chaos gate. Stamps {@code jbroker-from-broker-id}
 * metadata on every RPC so the peer's {@link ChaosServerInterceptor}
 * can decide whether to block; short-circuits locally when the
 * destination peer is in the caller's blocked set (prevents the RPC
 * from leaving the process under a partition).
 *
 * <p>Constructed per outbound channel bound to a specific peer id.
 */
public final class ChaosClientInterceptor implements ClientInterceptor {

    private final ChaosState state;
    private final int selfBrokerId;
    private final int peerBrokerId;

    public ChaosClientInterceptor(ChaosState state, int selfBrokerId, int peerBrokerId) {
        this.state = state;
        this.selfBrokerId = selfBrokerId;
        this.peerBrokerId = peerBrokerId;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        if (state.isPaused() || state.isOutboundBlocked(peerBrokerId)) {
            throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription(
                    "jbroker-chaos: outbound RPC to broker " + peerBrokerId + " blocked"));
        }
        state.maybeSleep();
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                headers.put(ChaosMetadataKeys.FROM_BROKER_ID, Integer.toString(selfBrokerId));
                super.start(responseListener, headers);
            }
        };
    }
}
