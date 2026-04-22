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
 * P8.6 — admin-app-side fan-in for broker event streams. Opens a
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
 * with a brief back-off. Redis pub/sub remains the optional production
 * path; Phase 8 ships with in-process fan-in only.
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
        var channel = io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
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
        long id = nextId.getAndIncrement();
        var e = new LocalEvent(id, brokerEndpoint, msg.getType(), msg.getDataJson(), msg.getId());
        synchronized (ringLock) {
            ring[(int) (id % RING_CAPACITY)] = e;
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
