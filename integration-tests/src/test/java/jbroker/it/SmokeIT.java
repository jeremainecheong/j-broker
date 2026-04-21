package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SmokeIT {
    @Test
    void moduleCompilesAndTestsRun() {
        assertThat(2 + 2).isEqualTo(4);
    }
}
