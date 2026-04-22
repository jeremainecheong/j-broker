package jbroker.admin.api;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import jbroker.admin.events.AdminEventBus;
import jbroker.admin.events.AdminEventBus.LocalEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * {@code /api/v1/events} — Server-Sent Events stream. Honors the
 * {@code Last-Event-ID} header for reconnects: the controller replays
 * every event in the admin-app-side ring buffer with id greater than the
 * client's last-seen value, then subscribes to live events.
 */
@RestController
@RequestMapping("/api/v1")
public class EventsController {

    private static final Logger log = LoggerFactory.getLogger(EventsController.class);

    private final AdminEventBus bus;

    public EventsController(AdminEventBus bus) {
        this.bus = bus;
    }

    @GetMapping(value = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter events(@RequestHeader(value = "Last-Event-ID", required = false) String lastEventId) {
        // Long (non-zero) timeout so SseEmitter doesn't auto-close an idle
        // connection mid-run. The container still tears it down on client
        // disconnect via onCompletion/onError callbacks below.
        var emitter = new SseEmitter(0L);
        var subscriberId = UUID.randomUUID().toString();
        var lastSent = new AtomicLong(parseLastEventId(lastEventId));

        // Replay any ring-buffer entries the client missed since its last
        // id. Replay runs BEFORE subscribing to live events so the client
        // sees a monotonic id stream with no gaps.
        try {
            for (LocalEvent e : bus.replayAfter(lastSent.get())) {
                send(emitter, e);
                lastSent.set(e.id());
            }
        } catch (IOException ioe) {
            log.debug("replay failed for subscriber {}: {}", subscriberId, ioe.getMessage());
            emitter.completeWithError(ioe);
            return emitter;
        }

        AdminEventBus.Subscriber sub = new AdminEventBus.Subscriber() {
            @Override
            public void onEvent(LocalEvent event) {
                if (event.id() <= lastSent.get()) return;
                try {
                    send(emitter, event);
                    lastSent.set(event.id());
                } catch (Exception e) {
                    // Client disconnected or send buffer overflowed; drop.
                    emitter.completeWithError(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                emitter.completeWithError(t);
            }

            @Override
            public void onClose() {
                emitter.complete();
            }
        };
        bus.subscribe(subscriberId, sub);
        emitter.onCompletion(() -> bus.unsubscribe(subscriberId));
        emitter.onTimeout(() -> bus.unsubscribe(subscriberId));
        emitter.onError(t -> bus.unsubscribe(subscriberId));

        return emitter;
    }

    private static void send(SseEmitter emitter, LocalEvent e) throws IOException {
        emitter.send(SseEmitter.event()
                .id(Long.toString(e.id()))
                .name(e.type())
                .data(e.dataJson(), MediaType.APPLICATION_JSON));
    }

    private static long parseLastEventId(String header) {
        if (header == null || header.isBlank()) return 0L;
        try {
            return Long.parseLong(header.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
