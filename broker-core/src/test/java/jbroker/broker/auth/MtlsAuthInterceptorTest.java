package jbroker.broker.auth;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;
import javax.security.auth.x500.X500Principal;
import org.junit.jupiter.api.Test;

class MtlsAuthInterceptorTest {

    /** Self-signed test cert, subject {@code O=jbroker-tests, CN=alice}, valid ~100 years. */
    private static final String ALICE_PEM =
            """
            -----BEGIN CERTIFICATE-----
            MIIDMzCCAhugAwIBAgIUQH3b9kV4qyTb7We087xZfBUnpOMwDQYJKoZIhvcNAQEL
            BQAwKDEWMBQGA1UECgwNamJyb2tlci10ZXN0czEOMAwGA1UEAwwFYWxpY2UwIBcN
            MjYwNzE4MTIzMDQzWhgPMjEyNjA2MjQxMjMwNDNaMCgxFjAUBgNVBAoMDWpicm9r
            ZXItdGVzdHMxDjAMBgNVBAMMBWFsaWNlMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A
            MIIBCgKCAQEAtT4of2w8k4DiURI33+8bxlIyX0jH8177Z05lKdxwhP5Z6+sC9O+f
            cCr52getqtEPyXjfnLcxXKRpkmGXayVzCDkhMMONTSRjJ8LHWWUYF9Gr+MdkxEz1
            v8YUWc1oZs6krChSsbewYcEf9moGF4vk06h0wjEuAiefJHkNvnRCMUdKo+TJKGXp
            Natb3lo9WwduwqLqpif2C+3qe8ehrgi32SBGvv0JgQczf70UVkX3or5d9cXfGu8I
            Rc2xQprZK1HRc/Qv5n+Xgo8pdET+r7gxTLFKNhyjM1d6mE3LB0zJdNuNvp6FLIGe
            PDDvNfwSWDHPZjr2rgMYMa8Azaoiwl+jMwIDAQABo1MwUTAdBgNVHQ4EFgQUftwU
            EM6NQay7MN/IdhNZnug2gv4wHwYDVR0jBBgwFoAUftwUEM6NQay7MN/IdhNZnug2
            gv4wDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEACcQifZakSrqM
            N0AZgwdY7sR6Pb3DGEZv5p95DdeF6nuk1x1Z1Z/Li6GZwTnyQhAbWZ/sa8EnrrXh
            TqlT0QD7RrCW6lwyY0qJTq6XaClnHrZRlqmI3PudqNd7rHx4pxz4EikrlCV3YcWT
            PAH01hYuABZZqgL6VMzhpPUiKCB6tdbDGi6rkw6BbkWVRQVB4Qe+VlmTuZMXHBqU
            967AVTTJHN2xnEJeQgDFjJcGkf/32GW9Exyrvk1ZSIUE4sffWn5D0LxxU4tTuzvR
            cbN+wuGcI6D+wFXyH/AAMs4CZaCVtv0zDW8KB4LjnwkRDPdMTApMVk9uU21crdv4
            Sc2Qg2K3/Q==
            -----END CERTIFICATE-----
            """;

    // ---------- CN extraction ----------

    @Test
    void cnComesOutOfAMultiAttributeSubject() {
        assertThat(MtlsAuthInterceptor.cnOf(new X500Principal("O=acme, CN=alice, C=SG")))
                .isEqualTo("alice");
    }

    @Test
    void subjectWithoutCnYieldsNoPrincipal() {
        assertThat(MtlsAuthInterceptor.cnOf(new X500Principal("O=acme, C=SG"))).isNull();
    }

    @Test
    void realCertificateYieldsItsCn() throws Exception {
        assertThat(MtlsAuthInterceptor.principalOf(sessionWith(aliceCert()))).isEqualTo("alice");
    }

    @Test
    void unverifiedPeerYieldsNoPrincipal() {
        assertThat(MtlsAuthInterceptor.principalOf(certlessSession())).isNull();
    }

    @Test
    void plaintextTransportYieldsNoPrincipal() {
        assertThat(MtlsAuthInterceptor.principalOf(null)).isNull();
    }

    // ---------- interceptor gating ----------

    @Test
    void mtlsModePublishesThePrincipalToTheHandler() throws Exception {
        var interceptor = new MtlsAuthInterceptor(AuthMode.MTLS);
        var seen = new AtomicReference<String>();
        interceptor.interceptCall(callWith(sessionWith(aliceCert())), new Metadata(), capturing(seen));

        assertThat(seen.get()).isEqualTo("alice");
    }

    @Test
    void mtlsModeRejectsCallsWithoutAClientCertificate() {
        var interceptor = new MtlsAuthInterceptor(AuthMode.MTLS);
        var call = callWith(certlessSession());
        var seen = new AtomicReference<String>();
        interceptor.interceptCall(call, new Metadata(), capturing(seen));

        assertThat(seen.get()).as("handler must not run").isNull();
        assertThat(call.closedWith).isNotNull();
        assertThat(call.closedWith.getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    }

    @Test
    void mtlsModeRejectsPlaintextCalls() {
        var interceptor = new MtlsAuthInterceptor(AuthMode.MTLS);
        var call = callWith(null);
        interceptor.interceptCall(call, new Metadata(), capturing(new AtomicReference<>()));

        assertThat(call.closedWith).isNotNull();
        assertThat(call.closedWith.getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    }

    @Test
    void noneModePassesAnonymousCallsThrough() {
        var interceptor = new MtlsAuthInterceptor(AuthMode.NONE);
        var call = callWith(null);
        var seen = new AtomicReference<String>();
        interceptor.interceptCall(call, new Metadata(), capturing(seen));

        assertThat(call.closedWith).isNull();
        assertThat(seen.get()).isEqualTo(AuthContext.ANONYMOUS);
    }

    @Test
    void noneModeStillNamesAnAuthenticatedPeer() throws Exception {
        var interceptor = new MtlsAuthInterceptor(AuthMode.NONE);
        var seen = new AtomicReference<String>();
        interceptor.interceptCall(callWith(sessionWith(aliceCert())), new Metadata(), capturing(seen));

        assertThat(seen.get()).isEqualTo("alice");
    }

    // ---------- fixtures ----------

    private static X509Certificate aliceCert() throws Exception {
        var in = new ByteArrayInputStream(ALICE_PEM.getBytes(StandardCharsets.US_ASCII));
        return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(in);
    }

    /** Handler that records what {@link AuthContext} resolves to when it runs. */
    private static ServerCallHandler<Void, Void> capturing(AtomicReference<String> seen) {
        return (call, headers) -> {
            seen.set(AuthContext.principalOrAnonymous());
            return new ServerCall.Listener<>() {};
        };
    }

    private static RecordingCall callWith(SSLSession session) {
        var attrs = session == null
                ? Attributes.EMPTY
                : Attributes.newBuilder()
                        .set(Grpc.TRANSPORT_ATTR_SSL_SESSION, session)
                        .build();
        return new RecordingCall(attrs);
    }

    private static final class RecordingCall extends ServerCall<Void, Void> {
        final Attributes attributes;
        Status closedWith;

        RecordingCall(Attributes attributes) {
            this.attributes = attributes;
        }

        @Override
        public Attributes getAttributes() {
            return attributes;
        }

        @Override
        public void close(Status status, Metadata trailers) {
            closedWith = status;
        }

        @Override
        public void request(int numMessages) {}

        @Override
        public void sendHeaders(Metadata headers) {}

        @Override
        public void sendMessage(Void message) {}

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public MethodDescriptor<Void, Void> getMethodDescriptor() {
            throw new UnsupportedOperationException("not needed by the interceptor");
        }
    }

    private static SSLSession sessionWith(X509Certificate leaf) {
        return new StubSslSession() {
            @Override
            public Certificate[] getPeerCertificates() {
                return new Certificate[] {leaf};
            }
        };
    }

    private static SSLSession certlessSession() {
        return new StubSslSession() {
            @Override
            public Certificate[] getPeerCertificates() throws SSLPeerUnverifiedException {
                throw new SSLPeerUnverifiedException("peer not authenticated");
            }
        };
    }

    /** Every method the interceptor doesn't touch throws. */
    private abstract static class StubSslSession implements SSLSession {
        @Override
        public byte[] getId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SSLSessionContext getSessionContext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getCreationTime() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLastAccessedTime() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void invalidate() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isValid() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putValue(String name, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getValue(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeValue(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String[] getValueNames() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Certificate[] getLocalCertificates() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Principal getPeerPrincipal() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Principal getLocalPrincipal() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getCipherSuite() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getProtocol() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getPeerHost() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getPeerPort() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getPacketBufferSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getApplicationBufferSize() {
            throw new UnsupportedOperationException();
        }
    }
}
