package jbroker.broker.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import jbroker.broker.ProtocolVersion;
import jbroker.proto.broker.AdminGrpc;
import jbroker.proto.broker.ApiVersionsRequest;
import jbroker.proto.broker.ApiVersionsResponse;
import jbroker.proto.broker.ListTopicsRequest;
import jbroker.proto.broker.ListTopicsResponse;
import jbroker.proto.broker.MetadataGrpc;
import jbroker.proto.common.ErrorCode;
import org.junit.jupiter.api.Test;

/**
 * {@link BrokerClient}'s protocol-range check: the first RPC on a client
 * triggers one {@code ApiVersions} handshake, an incompatible or absent
 * range surfaces as {@link UnsupportedBrokerException} before any real
 * RPC reaches the broker, and a compatible answer is checked exactly once
 * per client. Runs against a real loopback gRPC server so the check is
 * proven on the wire, not against a scripted transport.
 */
class BrokerClientHandshakeTest {

    /** Records call order; answers ListTopics with an empty OK response. */
    private static final class ScriptedAdmin extends AdminGrpc.AdminImplBase {
        final List<String> calls;

        ScriptedAdmin(List<String> calls) {
            this.calls = calls;
        }

        @Override
        public void listTopics(ListTopicsRequest req, StreamObserver<ListTopicsResponse> out) {
            calls.add("listTopics");
            out.onNext(ListTopicsResponse.newBuilder().build());
            out.onCompleted();
        }
    }

    private static final class ScriptedMetadata extends MetadataGrpc.MetadataImplBase {
        final List<String> calls;
        final AtomicInteger handshakes = new AtomicInteger();
        final int min;
        final int max;

        ScriptedMetadata(List<String> calls, int min, int max) {
            this.calls = calls;
            this.min = min;
            this.max = max;
        }

        @Override
        public void apiVersions(ApiVersionsRequest req, StreamObserver<ApiVersionsResponse> out) {
            calls.add("apiVersions");
            handshakes.incrementAndGet();
            out.onNext(ApiVersionsResponse.newBuilder()
                    .setError(ErrorCode.OK)
                    .setMinProtocolVersion(min)
                    .setMaxProtocolVersion(max)
                    .build());
            out.onCompleted();
        }
    }

    private static Server serve(io.grpc.BindableService... services) throws Exception {
        var b = NettyServerBuilder.forPort(0);
        for (var s : services) b.addService(s);
        return b.build().start();
    }

    @Test
    void firstRpcHandshakesOnceAndCompatibleRangePassesThrough() throws Exception {
        var calls = new CopyOnWriteArrayList<String>();
        var metadata = new ScriptedMetadata(calls, ProtocolVersion.MIN_SUPPORTED, ProtocolVersion.CURRENT);
        var server = serve(new ScriptedAdmin(calls), metadata);
        try (var client = new BrokerClient("127.0.0.1", server.getPort())) {
            assertThat(client.listTopics()).isEmpty();
            assertThat(client.listTopics()).isEmpty();

            assertThat(calls)
                    .as("handshake precedes the first real RPC and runs once per client")
                    .containsExactly("apiVersions", "listTopics", "listTopics");
            assertThat(metadata.handshakes.get()).isEqualTo(1);
        } finally {
            shutdown(server);
        }
    }

    @Test
    void disjointRangeFailsTheFirstRpcBeforeItReachesTheBroker() throws Exception {
        var calls = new CopyOnWriteArrayList<String>();
        var metadata = new ScriptedMetadata(calls, ProtocolVersion.CURRENT + 1, ProtocolVersion.CURRENT + 3);
        var server = serve(new ScriptedAdmin(calls), metadata);
        try (var client = new BrokerClient("127.0.0.1", server.getPort())) {
            assertThatThrownBy(client::listTopics).isInstanceOfSatisfying(UnsupportedBrokerException.class, ex -> {
                assertThat(ex.endpoint()).isEqualTo("127.0.0.1:" + server.getPort());
                assertThat(ex.brokerMin()).isEqualTo(ProtocolVersion.CURRENT + 1);
                assertThat(ex.brokerMax()).isEqualTo(ProtocolVersion.CURRENT + 3);
                assertThat(ex.clientMin()).isEqualTo(ProtocolVersion.MIN_SUPPORTED);
                assertThat(ex.clientMax()).isEqualTo(ProtocolVersion.CURRENT);
            });
            assertThat(calls)
                    .as("the real RPC never reaches an incompatible broker")
                    .containsExactly("apiVersions");
        } finally {
            shutdown(server);
        }
    }

    @Test
    void brokerWithoutApiVersionsRpcIsRejectedWithClearError() throws Exception {
        var calls = new CopyOnWriteArrayList<String>();
        // No Metadata service registered: ApiVersions answers UNIMPLEMENTED,
        // the shape an old broker without version discovery produces.
        var server = serve(new ScriptedAdmin(calls));
        try (var client = new BrokerClient("127.0.0.1", server.getPort())) {
            assertThatThrownBy(client::listTopics)
                    .isInstanceOf(UnsupportedBrokerException.class)
                    .hasMessageContaining("predates protocol version discovery")
                    .hasMessageContaining("127.0.0.1:" + server.getPort());
            assertThat(calls)
                    .as("no real RPC reaches a broker that cannot handshake")
                    .isEmpty();
        } finally {
            shutdown(server);
        }
    }

    @Test
    void failedHandshakeIsRetriedOnTheNextCall() throws Exception {
        var calls = new CopyOnWriteArrayList<String>();
        // First handshake answers a disjoint range, later ones a compatible
        // one — the shape of a broker upgraded in place between two calls.
        var metadata = new MetadataGrpc.MetadataImplBase() {
            final AtomicInteger handshakes = new AtomicInteger();

            @Override
            public void apiVersions(ApiVersionsRequest req, StreamObserver<ApiVersionsResponse> out) {
                calls.add("apiVersions");
                boolean first = handshakes.incrementAndGet() == 1;
                out.onNext(ApiVersionsResponse.newBuilder()
                        .setError(ErrorCode.OK)
                        .setMinProtocolVersion(first ? ProtocolVersion.CURRENT + 1 : ProtocolVersion.MIN_SUPPORTED)
                        .setMaxProtocolVersion(first ? ProtocolVersion.CURRENT + 1 : ProtocolVersion.CURRENT)
                        .build());
                out.onCompleted();
            }
        };
        var server = serve(new ScriptedAdmin(calls), metadata);
        try (var client = new BrokerClient("127.0.0.1", server.getPort())) {
            assertThatThrownBy(client::listTopics).isInstanceOf(UnsupportedBrokerException.class);

            // A failed check caches nothing: the same client re-handshakes
            // and works once the broker answers a compatible range.
            assertThat(client.listTopics()).isEmpty();
            assertThat(calls).containsExactly("apiVersions", "apiVersions", "listTopics");
        } finally {
            shutdown(server);
        }
    }

    private static void shutdown(Server server) throws InterruptedException {
        server.shutdownNow();
        server.awaitTermination(5, TimeUnit.SECONDS);
    }
}
