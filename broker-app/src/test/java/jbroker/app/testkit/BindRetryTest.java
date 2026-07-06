package jbroker.app.testkit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.BindException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BindRetryTest {

    @Test
    void retriesOnBindRaceAndReturnsResult() throws Exception {
        var attempts = new AtomicInteger();
        var result = BindRetry.startWithBindRetry(5, () -> {
            if (attempts.incrementAndGet() < 3) throw new BindException("Address already in use");
            return "ok";
        });
        assertThat(result).isEqualTo("ok");
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void nonBindFailuresPropagateImmediatelyWithoutRetry() {
        var attempts = new AtomicInteger();
        assertThatThrownBy(() -> BindRetry.startWithBindRetry(5, () -> {
                    attempts.incrementAndGet();
                    throw new IllegalStateException("boom");
                }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("boom");
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void exhaustedAttemptsThrowAssertionErrorWithLastBindFailureAsCause() {
        assertThatThrownBy(() -> BindRetry.startWithBindRetry(2, () -> {
                    throw new BindException("Address already in use");
                }))
                .isInstanceOf(AssertionError.class)
                .hasCauseInstanceOf(BindException.class);
    }

    @Test
    void bindRaceWrappedInCauseChainIsRetried() throws Exception {
        // gRPC/Netty wrap bind failures (IOException, NativeIoException);
        // detection must walk the cause chain, not just the top exception.
        var attempts = new AtomicInteger();
        int result = BindRetry.startWithBindRetry(5, () -> {
            if (attempts.incrementAndGet() < 2) {
                throw new IOException("Failed to bind", new BindException("Address already in use"));
            }
            return 42;
        });
        assertThat(result).isEqualTo(42);
        assertThat(attempts.get()).isEqualTo(2);
    }

    @Test
    void freePortsAllocatesDistinctPorts() {
        int[] ports = BindRetry.freePorts(6);
        assertThat(ports).hasSize(6);
        assertThat(java.util.Arrays.stream(ports).distinct().count()).isEqualTo(6);
        assertThat(java.util.Arrays.stream(ports).allMatch(p -> p > 0)).isTrue();
    }
}
