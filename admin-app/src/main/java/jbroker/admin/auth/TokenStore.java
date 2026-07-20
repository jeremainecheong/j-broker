package jbroker.admin.auth;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * In-memory bearer API tokens for the admin REST surface. Issued to a
 * logged-in operator, presented as {@code Authorization: Bearer <token>},
 * revocable, and expiring after {@code jbroker.admin.auth.token-ttl-seconds}
 * (default one day). Deliberately not persisted: an admin-app restart
 * invalidates outstanding tokens, which is the safe failure mode — callers
 * re-issue through the login flow.
 */
@Component
public class TokenStore {

    private record Grant(String user, long expiresAtMillis) {}

    private final SecureRandom random = new SecureRandom();
    private final Map<String, Grant> grants = new ConcurrentHashMap<>();
    private final long ttlMillis;

    public TokenStore(@Value("${jbroker.admin.auth.token-ttl-seconds:86400}") long ttlSeconds) {
        this.ttlMillis = ttlSeconds * 1_000L;
    }

    /** Mint a token for {@code user}. The returned string is the only copy. */
    public String issue(String user) {
        byte[] bytes = new byte[32];
        random.nextBytes(bytes);
        var token = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        grants.put(token, new Grant(user, System.currentTimeMillis() + ttlMillis));
        return token;
    }

    /** The owning user when the token exists and has not expired. */
    public Optional<String> userOf(String token) {
        var grant = grants.get(token);
        if (grant == null) return Optional.empty();
        if (System.currentTimeMillis() >= grant.expiresAtMillis()) {
            grants.remove(token);
            return Optional.empty();
        }
        return Optional.of(grant.user());
    }

    /** Revoke immediately. Unknown tokens are a no-op. */
    public void revoke(String token) {
        grants.remove(token);
    }
}
