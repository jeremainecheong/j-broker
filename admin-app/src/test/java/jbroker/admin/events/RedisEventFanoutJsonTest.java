package jbroker.admin.events;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * P13.7 — pure unit coverage of the hand-rolled JSON envelope. The
 * shape is narrow enough that a general-purpose JSON library would be
 * overkill in the hot pub/sub receive path, but narrow-shape parsers
 * bit-rot quickly without a test pinning the edge cases (embedded
 * quotes, newlines, unicode escapes).
 */
class RedisEventFanoutJsonTest {

    private final RedisEventFanout fanout = new RedisEventFanout(new AdminEventBus(null), "redis://localhost:6379");

    @Test
    void roundTripsSimpleEvent() {
        var e = new AdminEventBus.LocalEvent(42L, "127.0.0.1:9092", "leader_changed", "{\"partition\":0}", 17L);
        String encoded = fanout.encode(e);
        var decoded = fanout.decode(encoded);

        assertThat(decoded.podId()).isEqualTo(fanout.selfPodId());
        assertThat(decoded.brokerEndpoint()).isEqualTo("127.0.0.1:9092");
        assertThat(decoded.type()).isEqualTo("leader_changed");
        assertThat(decoded.dataJson()).isEqualTo("{\"partition\":0}");
        assertThat(decoded.brokerEventId()).isEqualTo(17L);
    }

    @Test
    void embeddedQuotesAndNewlinesSurviveRoundTrip() {
        var e = new AdminEventBus.LocalEvent(
                1L, "broker\"weird", "type\nwith\nnewlines", "{\"key\":\"value with \\\"nested\\\" quotes\"}", 99L);
        String encoded = fanout.encode(e);
        var decoded = fanout.decode(encoded);

        assertThat(decoded.brokerEndpoint()).isEqualTo("broker\"weird");
        assertThat(decoded.type()).isEqualTo("type\nwith\nnewlines");
        assertThat(decoded.dataJson()).isEqualTo("{\"key\":\"value with \\\"nested\\\" quotes\"}");
        assertThat(decoded.brokerEventId()).isEqualTo(99L);
    }

    @Test
    void controlCharactersAreUnicodeEscaped() {
        var e = new AdminEventBus.LocalEvent(1L, "endpoint", "type", "data-with--ctrl", 0L);
        String encoded = fanout.encode(e);
        assertThat(encoded).contains("\\u0001");
        var decoded = fanout.decode(encoded);
        assertThat(decoded.dataJson()).isEqualTo("data-with--ctrl");
    }
}
