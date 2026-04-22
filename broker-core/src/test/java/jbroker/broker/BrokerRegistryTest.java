package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BrokerRegistryTest {

    @Test
    void lookupUnknownReturnsEmpty() {
        var reg = new BrokerRegistry();
        assertThat(reg.addressFor(42)).isEmpty();
    }

    @Test
    void applyThenLookup() {
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(1, "h1", 9001);
        assertThat(reg.addressFor(1)).contains(new BrokerRegistry.HostPort("h1", 9001));
    }

    @Test
    void reRegistrationOverwritesPreviousAddress() {
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(1, "h1", 9001);
        reg.onBrokerRegistration(1, "h1-new", 9009);
        assertThat(reg.addressFor(1)).contains(new BrokerRegistry.HostPort("h1-new", 9009));
    }

    @Test
    void knownBrokerIdsListsEveryAppliedRegistration() {
        var reg = new BrokerRegistry();
        reg.onBrokerRegistration(1, "h1", 9001);
        reg.onBrokerRegistration(2, "h2", 9002);
        reg.onBrokerRegistration(3, "h3", 9003);
        assertThat(reg.knownBrokerIds()).containsExactlyInAnyOrder(1, 2, 3);
    }
}
