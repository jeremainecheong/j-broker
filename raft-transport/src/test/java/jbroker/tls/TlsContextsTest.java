package jbroker.tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class TlsContextsTest {

    @Test
    void disabledConfigYieldsNullContexts() throws Exception {
        assertThat(TlsContexts.serverContext(TlsConfig.DISABLED)).isNull();
        assertThat(TlsContexts.clientContext(TlsConfig.DISABLED)).isNull();
        assertThat(TlsContexts.serverContext(null)).isNull();
        assertThat(TlsContexts.clientContext(null)).isNull();
    }

    @Test
    void serverConfigRequiresCertAndKey() {
        assertThatThrownBy(() -> new TlsConfig(true, null, null, Path.of("ca.crt"), true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("client-auth required");
    }

    @Test
    void enabledConfigRequiresTrustStore() {
        assertThatThrownBy(() -> new TlsConfig(true, Path.of("c.crt"), Path.of("c.key"), null, false))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void certAndKeyMustBeSetTogether() {
        assertThatThrownBy(() -> new TlsConfig(true, Path.of("c.crt"), null, Path.of("ca.crt"), false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("certChain and privateKey must be set together");
    }
}
