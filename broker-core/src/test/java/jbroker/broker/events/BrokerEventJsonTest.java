package jbroker.broker.events;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pin the snake_case contract for the SSE event payloads. The
 * admin-app's /api/v1/events stream passes these strings through
 * verbatim, so the keys here are on the public wire and must match the
 * rest of the admin REST envelope.
 */
final class BrokerEventJsonTest {

    @Test
    void leaderChangedUsesSnakeCase() {
        var json = BrokerEventJson.encode(new BrokerEvent.LeaderChanged(1, "orders", 3, 1, 2, 7));
        assertThat(json)
                .contains("\"old_leader\":1")
                .contains("\"new_leader\":2")
                .contains("\"leader_epoch\":7");
    }

    @Test
    void brokerRegisteredUsesSnakeCase() {
        var json = BrokerEventJson.encode(new BrokerEvent.BrokerRegistered(1, 3, "broker3", 9092));
        assertThat(json)
                .contains("\"broker_id\":3")
                .contains("\"host\":\"broker3\"")
                .contains("\"port\":9092");
    }

    @Test
    void brokerFencedUsesSnakeCase() {
        var json = BrokerEventJson.encode(new BrokerEvent.BrokerFenced(1, 3));
        assertThat(json).isEqualTo("{\"broker_id\":3}");
    }

    @Test
    void raftTermChangedUsesSnakeCase() {
        var json = BrokerEventJson.encode(new BrokerEvent.RaftTermChanged(1, 4, 5, 2));
        assertThat(json).contains("\"old_term\":4").contains("\"new_term\":5").contains("\"new_leader\":2");
    }

    @Test
    void consumerGroupRebalanceUsesSnakeCase() {
        var json = BrokerEventJson.encode(new BrokerEvent.ConsumerGroupRebalance(1, "analytics", 7, 4));
        assertThat(json)
                .contains("\"group_id\":\"analytics\"")
                .contains("\"generation\":7")
                .contains("\"member_count\":4");
    }

    @Test
    void isrChangedUsesSnakeCase() {
        var json = BrokerEventJson.encode(new BrokerEvent.IsrChanged(1, "orders", 3, List.of(1, 2, 3), false));
        assertThat(json)
                .contains("\"topic\":\"orders\"")
                .contains("\"partition\":3")
                .contains("\"isr\":[1,2,3]");
    }

    @Test
    void chaosActionOmitsNullOptionalFields() {
        var kill = BrokerEventJson.encode(new BrokerEvent.ChaosAction(1, "kill", 2, null, null));
        assertThat(kill).isEqualTo("{\"action\":\"kill\",\"broker_id\":2}");

        var partition = BrokerEventJson.encode(new BrokerEvent.ChaosAction(2, "partition", 1, 3, null));
        assertThat(partition).contains("\"peer_id\":3").doesNotContain("\"millis\"");

        var latency = BrokerEventJson.encode(new BrokerEvent.ChaosAction(3, "inject_latency", 2, null, 50L));
        assertThat(latency).contains("\"millis\":50").doesNotContain("\"peer_id\"");
    }

    @Test
    void noEventUsesCamelCaseKeys() {
        // Regression guard — if someone reintroduces camelCase in a future
        // event variant, this test fails before it ships.
        var all = List.of(
                BrokerEventJson.encode(new BrokerEvent.LeaderChanged(1, "t", 0, 1, 2, 3)),
                BrokerEventJson.encode(new BrokerEvent.BrokerRegistered(1, 1, "h", 1)),
                BrokerEventJson.encode(new BrokerEvent.BrokerFenced(1, 1)),
                BrokerEventJson.encode(new BrokerEvent.RaftTermChanged(1, 1, 2, 1)),
                BrokerEventJson.encode(new BrokerEvent.ConsumerGroupRebalance(1, "g", 1, 1)),
                BrokerEventJson.encode(new BrokerEvent.IsrChanged(1, "t", 0, List.of(1), false)),
                BrokerEventJson.encode(new BrokerEvent.ChaosAction(1, "kill", 1, 2, 3L)));
        for (var s : all) {
            assertThat(s)
                    .doesNotContain("brokerId")
                    .doesNotContain("oldLeader")
                    .doesNotContain("newLeader")
                    .doesNotContain("leaderEpoch")
                    .doesNotContain("oldTerm")
                    .doesNotContain("newTerm")
                    .doesNotContain("groupId")
                    .doesNotContain("memberCount")
                    .doesNotContain("peerId");
        }
    }
}
