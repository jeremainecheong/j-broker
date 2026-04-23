package jbroker.broker.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class ChaosServerInterceptorTest {

    @SuppressWarnings("unchecked")
    private static <ReqT, RespT> ServerCall<ReqT, RespT> recordingCall(AtomicReference<Status> out) {
        return (ServerCall<ReqT, RespT>) new ServerCall<Object, Object>() {
            @Override
            public void request(int numMessages) {}

            @Override
            public void sendHeaders(Metadata headers) {}

            @Override
            public void sendMessage(Object message) {}

            @Override
            public void close(Status status, Metadata trailers) {
                out.set(status);
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public io.grpc.MethodDescriptor<Object, Object> getMethodDescriptor() {
                return null;
            }
        };
    }

    @Test
    void pausedStatePumpsUnavailable() {
        var state = new ChaosState();
        state.pause();
        var interceptor = new ChaosServerInterceptor(state);
        var closed = new AtomicReference<Status>();
        ServerCall<Object, Object> call = recordingCall(closed);
        ServerCallHandler<Object, Object> handler = (c, h) -> {
            throw new AssertionError("handler should not be called when paused");
        };
        interceptor.interceptCall(call, new Metadata(), handler);
        assertThat(closed.get().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    }

    @Test
    void blockedPeerCausesUnavailable() {
        var state = new ChaosState();
        state.blockPeer(2);
        var interceptor = new ChaosServerInterceptor(state);
        var closed = new AtomicReference<Status>();
        ServerCall<Object, Object> call = recordingCall(closed);
        var headers = new Metadata();
        headers.put(ChaosMetadataKeys.FROM_BROKER_ID, "2");
        interceptor.interceptCall(call, headers, (c, h) -> {
            throw new AssertionError("handler should not be called for blocked peer");
        });
        assertThat(closed.get().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    }

    @Test
    void unblockedPeerPassesThroughToHandler() {
        var state = new ChaosState();
        var interceptor = new ChaosServerInterceptor(state);
        var closed = new AtomicReference<Status>();
        ServerCall<Object, Object> call = recordingCall(closed);
        var headers = new Metadata();
        headers.put(ChaosMetadataKeys.FROM_BROKER_ID, "3");
        var invoked = new java.util.concurrent.atomic.AtomicBoolean();
        interceptor.interceptCall(call, headers, (c, h) -> {
            invoked.set(true);
            return new ServerCall.Listener<>() {};
        });
        assertThat(invoked.get()).isTrue();
        assertThat(closed.get()).isNull();
    }

    @Test
    void missingFromHeaderDoesNotBlock() {
        var state = new ChaosState();
        state.blockPeer(9); // irrelevant: header absent
        var interceptor = new ChaosServerInterceptor(state);
        var closed = new AtomicReference<Status>();
        ServerCall<Object, Object> call = recordingCall(closed);
        var invoked = new java.util.concurrent.atomic.AtomicBoolean();
        interceptor.interceptCall(call, new Metadata(), (c, h) -> {
            invoked.set(true);
            return new ServerCall.Listener<>() {};
        });
        assertThat(invoked.get()).isTrue();
    }
}
