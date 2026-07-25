package jbroker.broker.txn;

import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import jbroker.broker.BrokerRegistry;
import jbroker.proto.txn.TxnMarkersGrpc;
import jbroker.proto.txn.WriteTxnMarkersRequest;
import jbroker.proto.txn.WriteTxnMarkersResponse;
import jbroker.tls.TlsConfig;
import jbroker.tls.TlsContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-peer channel cache for the {@code TxnMarkers.WriteTxnMarkers}
 * inter-broker RPC. Marker traffic is low-volume and fully retried by the
 * coordinator's delivery loop, so the failure policy is deliberately
 * blunt: any RPC failure drops the cached channel and the next attempt
 * redials — no backoff bookkeeping to fight (the delivery loop already
 * paces retries). Addresses are re-resolved per call so leadership moves
 * and re-registrations are picked up without invalidation plumbing;
 * inter-broker dialing uses the internal (not advertised) address, same
 * as replication.
 */
public final class TxnMarkerPeerClients implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TxnMarkerPeerClients.class);

    /**
     * Per-RPC deadline. Must exceed the receiver's post-append replication
     * wait ({@link TxnMarkerWriter#REPLICATION_TIMEOUT_MS}) or a marker
     * that IS landing would be reported failed and redelivered.
     */
    static final long RPC_TIMEOUT_MS = TxnMarkerWriter.REPLICATION_TIMEOUT_MS + 3_000L;

    private record Entry(String host, int port, ManagedChannel channel, TxnMarkersGrpc.TxnMarkersBlockingStub stub) {}

    private final IntFunction<Optional<BrokerRegistry.HostPort>> addressResolver;
    private final io.grpc.netty.shaded.io.netty.handler.ssl.SslContext sslCtx;
    private final IntFunction<ClientInterceptor> interceptorFactory;
    private final ConcurrentHashMap<Integer, Entry> channels = new ConcurrentHashMap<>();
    private volatile boolean closed;

    public TxnMarkerPeerClients(
            TlsConfig tls,
            IntFunction<ClientInterceptor> interceptorFactory,
            IntFunction<Optional<BrokerRegistry.HostPort>> addressResolver) {
        try {
            this.sslCtx = TlsContexts.clientContext(tls);
        } catch (javax.net.ssl.SSLException e) {
            throw new IllegalStateException("TLS client context build failed", e);
        }
        this.interceptorFactory = interceptorFactory;
        this.addressResolver = addressResolver;
    }

    /**
     * One marker write to {@code brokerId}. Empty when the broker has no
     * registered address or the RPC failed — the caller's retry loop
     * re-resolves the leader and tries again.
     */
    public Optional<WriteTxnMarkersResponse> write(int brokerId, WriteTxnMarkersRequest req) {
        var address = addressResolver.apply(brokerId);
        if (address.isEmpty()) return Optional.empty();
        var entry = entryFor(brokerId, address.get());
        if (entry == null) return Optional.empty();
        try {
            return Optional.of(entry.stub()
                    .withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .writeTxnMarkers(req));
        } catch (RuntimeException e) {
            log.debug("WriteTxnMarkers to broker {} failed; dropping channel: {}", brokerId, e.toString());
            dropChannel(brokerId, entry);
            return Optional.empty();
        }
    }

    private Entry entryFor(int brokerId, BrokerRegistry.HostPort address) {
        if (closed) return null;
        var entry = channels.compute(brokerId, (id, cached) -> {
            if (cached != null && cached.host().equals(address.host()) && cached.port() == address.port()) {
                return cached;
            }
            if (cached != null) cached.channel().shutdownNow();
            var b = NettyChannelBuilder.forAddress(address.host(), address.port());
            if (sslCtx == null) {
                b.usePlaintext();
            } else {
                b.sslContext(sslCtx);
            }
            var interceptor = interceptorFactory == null ? null : interceptorFactory.apply(id);
            if (interceptor != null) b.intercept(interceptor);
            var channel = b.build();
            return new Entry(address.host(), address.port(), channel, TxnMarkersGrpc.newBlockingStub(channel));
        });
        if (closed) {
            // Raced close(): make sure nothing lingers.
            dropChannel(brokerId, entry);
            return null;
        }
        return entry;
    }

    private void dropChannel(int brokerId, Entry entry) {
        if (channels.remove(brokerId, entry)) {
            entry.channel().shutdownNow();
        }
    }

    @Override
    public void close() {
        closed = true;
        for (var id : channels.keySet()) {
            var entry = channels.remove(id);
            if (entry != null) entry.channel().shutdownNow();
        }
    }
}
