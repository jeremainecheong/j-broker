package jbroker.admin.events;

import io.grpc.stub.StreamObserver;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import jbroker.admin.client.BrokerAdminClientPool;
import jbroker.proto.broker.EventMessage;
import jbroker.proto.broker.MetadataGrpc;
import jbroker.proto.broker.SubscribeEventsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Admin-app-side fan-in for broker event streams. Opens a
 * {@code SubscribeEvents} RPC against every configured broker; each
 * received event is:
 * <ul>
 *   <li>Re-keyed under a admin-app-local monotonic id (broker ids aren't
 *       globally unique, and a single admin-app fronts N brokers).</li>
 *   <li>Appended to a 2048-slot ring for {@code Last-Event-ID} replay.</li>
 *   <li>Fanned out to every live SSE subscriber.</li>
 * </ul>
 *
 * <p>When a broker stream errors or closes, the bus retries the connection
 * with a brief back-off. The {@link EventFanout} seam decides whether
 * ingested events also propagate to peer admin replicas (Redis) or stay
 * in-process (the default, correct for a single replica).
 */
@Component
public class AdminEventBus {

    private static final Logger log = LoggerFactory.getLogger(AdminEventBus.class);
    private static final int RING_CAPACITY = 2048;

    public record LocalEvent(long id, String brokerEndpoint, String type, String dataJson, long brokerEventId) {}

    public interface Subscriber {
        void onEvent(LocalEvent event);

        default void onError(Throwable t) {}

        default void onClose() {}
    }

    private final BrokerAdminClientPool pool;
    private final LocalEvent[] ring = new LocalEvent[RING_CAPACITY];
    private final AtomicLong nextId = new AtomicLong(1);
    private final Object ringLock = new Object();
    private final List<io.grpc.ManagedChannel> streamChannels = new ArrayList<>();
    private final ConcurrentHashMap<String, Subscriber> subscribers = new ConcurrentHashMap<>();
    private volatile boolean running = true;

    /**
     * Optional side-channel publisher, invoked for every event the
     * bus ingests (both from broker subscriptions and from
     * {@link #injectExternal}-cycled Redis echoes are intentionally NOT
     * re-published; see {@code injectExternal}). When null the bus runs
     * in pure in-process mode.
     */
    private volatile java.util.function.Consumer<LocalEvent> externalPublisher;

    /**
     * Dedupe set keyed on {@code (brokerEndpoint, brokerEventId)}.
     * Shared by direct broker ingestion and external Redis injection so a
     * pod never double-broadcasts the same broker event regardless of
     * whether it arrived via its own gRPC stream or via another pod's
     * Redis publish.
     */
    private final java.util.Set<String> seenBrokerEvents =
            java.util.Collections.newSetFromMap(new java.util.LinkedHashMap<>() {
                @Override
                protected boolean removeEldestEntry(java.util.Map.Entry<String, Boolean> eldest) {
                    return size() > RING_CAPACITY;
                }
            });

    @Autowired
    public AdminEventBus(BrokerAdminClientPool pool) {
        this.pool = pool;
    }

    @PostConstruct
    public void start() {
        for (var client : pool.clients()) {
            connect(client.address());
        }
    }

    @PreDestroy
    public void stop() {
        running = false;
        for (var ch : streamChannels) {
            ch.shutdownNow();
        }
    }

    private void connect(String address) {
        int colon = address.indexOf(':');
        String host = address.substring(0, colon);
        int port = Integer.parseInt(address.substring(colon + 1));
        var b = io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder.forAddress(host, port);
        try {
            var sslCtx = jbroker.tls.TlsContexts.clientContext(pool.tls());
            if (sslCtx == null) {
                b.usePlaintext();
            } else {
                b.sslContext(sslCtx);
            }
        } catch (javax.net.ssl.SSLException e) {
            throw new IllegalStateException("TLS client context build failed", e);
        }
        var channel = b.build();
        synchronized (streamChannels) {
            streamChannels.add(channel);
        }
        var stub = MetadataGrpc.newStub(channel);
        stub.subscribeEvents(SubscribeEventsRequest.newBuilder().setAfterId(0L).build(), new StreamObserver<>() {
            @Override
            public void onNext(EventMessage value) {
                ingest(address, value);
            }

            @Override
            public void onError(Throwable t) {
                if (!running) return;
                log.debug("event stream to {} errored: {}", address, t.getMessage());
                // Brief back-off before reconnect so we don't spin
                // on a broker that's down.
                Thread.ofVirtual().start(() -> {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (running) connect(address);
                });
            }

            @Override
            public void onCompleted() {
                log.debug("event stream to {} completed", address);
                if (running) {
                    Thread.ofVirtual().start(() -> {
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        if (running) connect(address);
                    });
                }
            }
        });
    }

    private void ingest(String brokerEndpoint, EventMessage msg) {
        // Dedupe before allocating a LocalEvent so we don't bump
        // nextId for events the bus has already broadcast (either from a
        // redundant broker stream or from a Redis echo).
        String dedupeKey = brokerEndpoint + "#" + msg.getId();
        synchronized (seenBrokerEvents) {
            if (!seenBrokerEvents.add(dedupeKey)) return;
        }
        long id = nextId.getAndIncrement();
        var e = new LocalEvent(id, brokerEndpoint, msg.getType(), msg.getDataJson(), msg.getId());
        broadcast(e);
        // Publish to the external (Redis) channel AFTER local broadcast so
        // local subscribers always see fresh events first.
        var pub = externalPublisher;
        if (pub != null) {
            try {
                pub.accept(e);
            } catch (Exception err) {
                log.debug("external publisher threw; dropping: {}", err.getMessage());
            }
        }
    }

    /**
     * Inject an event delivered via the external pub/sub channel
     * (typically Redis pub/sub from a peer admin pod). De-duplicates on
     * {@code (brokerEndpoint, brokerEventId)} so the same broker event
     * arriving via a peer's Redis publish AND this pod's direct gRPC
     * subscription is broadcast exactly once. Does NOT bounce back out
     * to the external publisher — the peer already published it.
     */
    public void injectExternal(String brokerEndpoint, String type, String dataJson, long brokerEventId) {
        String dedupeKey = brokerEndpoint + "#" + brokerEventId;
        synchronized (seenBrokerEvents) {
            if (!seenBrokerEvents.add(dedupeKey)) return;
        }
        long id = nextId.getAndIncrement();
        broadcast(new LocalEvent(id, brokerEndpoint, type, dataJson, brokerEventId));
    }

    /**
     * Install a fan-out hook. Invoked for every new event the
     * bus emits locally. Passing null disables the hook (tests use this
     * to tear down the Redis wiring in {@code PreDestroy}).
     */
    public void setExternalPublisher(java.util.function.Consumer<LocalEvent> publisher) {
        this.externalPublisher = publisher;
    }

    private void broadcast(LocalEvent e) {
        synchronized (ringLock) {
            ring[(int) (e.id() % RING_CAPACITY)] = e;
        }
        for (var s : subscribers.values()) {
            try {
                s.onEvent(e);
            } catch (Exception err) {
                log.debug("subscriber threw; dropping: {}", err.getMessage());
            }
        }
    }

    /** Replay every event with id > {@code lastSeen} that's still in the ring. */
    public List<LocalEvent> replayAfter(long lastSeen) {
        var out = new ArrayList<LocalEvent>();
        synchronized (ringLock) {
            for (var e : ring) {
                if (e == null) continue;
                if (e.id() > lastSeen) out.add(e);
            }
        }
        out.sort((a, b) -> Long.compare(a.id(), b.id()));
        return out;
    }

    public void subscribe(String id, Subscriber s) {
        subscribers.put(id, s);
    }

    public void unsubscribe(String id) {
        subscribers.remove(id);
    }
}
