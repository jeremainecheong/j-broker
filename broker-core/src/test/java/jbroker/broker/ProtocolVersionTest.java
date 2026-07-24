package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class ProtocolVersionTest {

    @Test
    void brokerVersionIsNeverBlank() {
        assertThat(ProtocolVersion.BROKER_VERSION).isNotBlank();
        assertThat(ProtocolVersion.BUILD_COMMIT).isNotBlank();
    }

    @Test
    void brokerVersionMatchesStampedResourceWhenPresent() throws IOException {
        try (InputStream in = ProtocolVersion.class.getResourceAsStream(ProtocolVersion.VERSION_RESOURCE)) {
            assumeTrue(in != null, "stamped resource absent; the compiled-in fallback applies");
            var props = new Properties();
            props.load(in);
            assertThat(ProtocolVersion.BROKER_VERSION).isEqualTo(props.getProperty("version"));
            assertThat(ProtocolVersion.BUILD_COMMIT).isEqualTo(props.getProperty("commit"));
        }
    }
}
