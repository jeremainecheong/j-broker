package jbroker.broker.auth;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

/**
 * Authentication gate on the broker listener. Pulls the peer's TLS
 * session off the transport, extracts the CN of the client certificate,
 * and publishes it as the request principal via {@link AuthContext}.
 *
 * <p>Under {@link AuthMode#MTLS} an RPC without a verifiable client
 * certificate is closed with {@link Status#UNAUTHENTICATED} before any
 * handler runs. This holds even when the TLS listener itself accepts
 * certless handshakes (client-auth optional), so the reject surfaces as a
 * clean gRPC status instead of a mid-handshake connection reset. Under
 * {@link AuthMode#NONE} everything passes; a certificate still yields a
 * principal so quota accounting names real identities where possible.
 */
public final class MtlsAuthInterceptor implements ServerInterceptor {

    private final AuthMode mode;

    public MtlsAuthInterceptor(AuthMode mode) {
        this.mode = mode;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String principal = principalOf(call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION));
        if (principal == null) {
            if (mode == AuthMode.MTLS) {
                call.close(
                        Status.UNAUTHENTICATED.withDescription("auth.mode=mtls: client certificate required"),
                        new Metadata());
                return new ServerCall.Listener<>() {};
            }
            return next.startCall(call, headers);
        }
        Context ctx = Context.current().withValue(AuthContext.PRINCIPAL, principal);
        return Contexts.interceptCall(ctx, call, headers, next);
    }

    /** CN of the peer's verified client certificate, or null when absent. */
    static String principalOf(SSLSession session) {
        if (session == null) return null;
        Certificate[] chain;
        try {
            chain = session.getPeerCertificates();
        } catch (SSLPeerUnverifiedException e) {
            return null; // TLS without client auth — no identity to extract
        }
        if (chain.length == 0 || !(chain[0] instanceof X509Certificate leaf)) return null;
        return cnOf(leaf.getSubjectX500Principal());
    }

    /** First CN attribute of the subject DN, or null when the DN has none. */
    static String cnOf(X500Principal subject) {
        try {
            for (var rdn : new LdapName(subject.getName(X500Principal.RFC2253)).getRdns()) {
                if ("CN".equalsIgnoreCase(rdn.getType())) {
                    return rdn.getValue().toString();
                }
            }
        } catch (InvalidNameException e) {
            return null; // unparseable DN — treat as no identity
        }
        return null;
    }
}
