package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import jbroker.proto.broker.BrokerInfo;
import jbroker.proto.broker.DescribeClusterResponse;
import org.junit.jupiter.api.Test;

/**
 * The pure half of the CLI's leader-hint follow: pull the leader id out
 * of a NOT_LEADER-shaped error message and resolve it against a cluster
 * view. Anything without a "leader is broker N" id — election windows,
 * unknown topics, transport noise — must resolve to null so the CLI
 * surfaces the original error instead of redialing blindly.
 */
class BrokerAppLeaderHintTest {

    private static DescribeClusterResponse view() {
        return DescribeClusterResponse.newBuilder()
                .addNodes(BrokerInfo.newBuilder()
                        .setBrokerId(1)
                        .setHost("127.0.0.1")
                        .setPort(9092)
                        .build())
                .addNodes(BrokerInfo.newBuilder()
                        .setBrokerId(2)
                        .setHost("broker2.internal")
                        .setPort(9093)
                        .build())
                .addNodes(BrokerInfo.newBuilder().setBrokerId(3).build()) // no advertised address yet
                .build();
    }

    @Test
    void controllerRefusalResolvesToTheNamedBroker() {
        var ep = BrokerApp.suggestedLeader("createTopic failed: not the controller; leader is broker 2", view());
        assertThat(ep).isNotNull();
        assertThat(ep.host()).isEqualTo("broker2.internal");
        assertThat(ep.port()).isEqualTo(9093);
    }

    @Test
    void produceRefusalResolvesToTheNamedBroker() {
        var ep = BrokerApp.suggestedLeader("produce failed: leader is broker 1 for orders-0", view());
        assertThat(ep).isNotNull();
        assertThat(ep.host()).isEqualTo("127.0.0.1");
        assertThat(ep.port()).isEqualTo(9092);
    }

    @Test
    void messagesWithoutALeaderIdResolveToNull() {
        assertThat(BrokerApp.suggestedLeader("createTopic failed: no Raft leader elected yet", view()))
                .isNull();
        assertThat(BrokerApp.suggestedLeader("produce failed: no leader for partition orders-0", view()))
                .isNull();
        assertThat(BrokerApp.suggestedLeader("fetch failed: unknown topic orders", view()))
                .isNull();
    }

    @Test
    void leaderMissingFromTheViewOrWithoutAnAddressResolvesToNull() {
        assertThat(BrokerApp.suggestedLeader("produce failed: leader is broker 9 for orders-0", view()))
                .isNull();
        assertThat(BrokerApp.suggestedLeader("produce failed: leader is broker 3 for orders-0", view()))
                .as("a node without an advertised endpoint is not dialable")
                .isNull();
    }
}
