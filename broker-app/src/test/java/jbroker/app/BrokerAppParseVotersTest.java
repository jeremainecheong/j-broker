package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * Covers every branch of {@link BrokerApp#parseVoters(String)}. The
 * CLI relies on this for multi-broker docker-compose bootstrap, so a silent
 * parse failure here mis-routes the whole cluster.
 */
final class BrokerAppParseVotersTest {

    @Test
    void singleVoterRoundTrips() {
        var voters = BrokerApp.parseVoters("1@broker1:9192:9092");
        assertThat(voters).hasSize(1);
        var v = voters.get(0);
        assertThat(v.id().value()).isEqualTo(1);
        assertThat(v.host()).isEqualTo("broker1");
        assertThat(v.raftPort()).isEqualTo(9192);
        assertThat(v.brokerPort()).isEqualTo(9092);
    }

    @Test
    void threeVotersPreserveOrder() {
        var voters = BrokerApp.parseVoters("1@broker1:9192:9092,2@broker2:9192:9092,3@broker3:9192:9092");
        assertThat(voters).extracting(v -> v.id().value()).containsExactly(1, 2, 3);
        assertThat(voters).extracting(VoterAddress::host).containsExactly("broker1", "broker2", "broker3");
    }

    @Test
    void whitespaceBetweenEntriesIsTolerated() {
        var voters = BrokerApp.parseVoters("  1@h1:1:2 ,  2@h2:3:4 ");
        assertThat(voters).hasSize(2);
    }

    @Test
    void emptyEntriesAreSkipped() {
        var voters = BrokerApp.parseVoters("1@h:1:2,,,2@k:3:4");
        assertThat(voters).hasSize(2);
    }

    @Test
    void missingAtRejected() {
        assertThatThrownBy(() -> BrokerApp.parseVoters("1broker:1:2"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ID@HOST:RAFT:BROKER");
    }

    @Test
    void emptyIdRejected() {
        assertThatThrownBy(() -> BrokerApp.parseVoters("@host:1:2"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ID@HOST:RAFT:BROKER");
    }

    @Test
    void emptyHostRejected() {
        assertThatThrownBy(() -> BrokerApp.parseVoters("1@:1:2"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-blank");
    }

    @Test
    void wrongPortCountRejected() {
        assertThatThrownBy(() -> BrokerApp.parseVoters("1@host:1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ID@HOST:RAFT:BROKER");
    }

    @Test
    void nonIntegerPortRejectedWithContext() {
        assertThatThrownBy(() -> BrokerApp.parseVoters("1@host:abc:9092"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("raft port")
                .hasMessageContaining("abc");
    }

    @Test
    void nonIntegerIdRejectedWithContext() {
        assertThatThrownBy(() -> BrokerApp.parseVoters("foo@host:1:2"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("voter id")
                .hasMessageContaining("foo");
    }

    @Test
    void duplicateVoterIdsRejected() {
        assertThatThrownBy(() -> BrokerApp.parseVoters("1@a:1:2,1@b:3:4"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate voter id 1");
    }

    @Test
    void blankSpecRejected() {
        assertThatThrownBy(() -> BrokerApp.parseVoters("   ,  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one entry");
    }
}
