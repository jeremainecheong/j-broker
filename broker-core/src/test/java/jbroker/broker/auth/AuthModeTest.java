package jbroker.broker.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class AuthModeTest {

    @Test
    void parsesBothModesCaseInsensitively() {
        assertThat(AuthMode.parse("none")).isEqualTo(AuthMode.NONE);
        assertThat(AuthMode.parse("MTLS")).isEqualTo(AuthMode.MTLS);
        assertThat(AuthMode.parse("mTls")).isEqualTo(AuthMode.MTLS);
    }

    @Test
    void rejectsUnknownValuesByName() {
        assertThatThrownBy(() -> AuthMode.parse("scram"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scram");
    }
}
