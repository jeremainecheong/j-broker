package jbroker.admin.auth;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Component;

/**
 * Operator accounts for the admin UI/API, parsed from
 * {@code jbroker.admin.auth.users} — comma-separated {@code name:bcrypt-hash}
 * pairs (bcrypt hashes contain no commas or colons beyond their {@code $}
 * framing, so the format is unambiguous). Empty value ⇒ auth is DISABLED:
 * every request passes, matching pre-auth deployments, and startup logs a
 * warning so an exposed admin-app can't miss it.
 *
 * <p>Generate a hash with
 * {@code htpasswd -bnBC 10 "" 's3cret' | tr -d ':\n'} or any bcrypt tool.
 */
@Component
public class AdminUsers {

    private static final Logger log = LoggerFactory.getLogger(AdminUsers.class);

    private final Map<String, String> hashesByUser = new HashMap<>();
    private final BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();

    public AdminUsers(@Value("${jbroker.admin.auth.users:}") String users) {
        for (var pair : users.split(",")) {
            if (pair.isBlank()) continue;
            int sep = pair.indexOf(':');
            if (sep <= 0 || sep == pair.length() - 1) {
                throw new IllegalArgumentException(
                        "jbroker.admin.auth.users entries must be name:bcrypt-hash, got: " + pair.trim());
            }
            hashesByUser.put(
                    pair.substring(0, sep).trim(), pair.substring(sep + 1).trim());
        }
        if (hashesByUser.isEmpty()) {
            log.warn("jbroker.admin.auth.users is empty — admin UI/API authentication is DISABLED; "
                    + "configure operator accounts before exposing this app to a real network");
        }
    }

    /** Auth is enforced only when at least one account exists. */
    public boolean enabled() {
        return !hashesByUser.isEmpty();
    }

    /** Constant-shape bcrypt check; false for unknown users. */
    public boolean authenticate(String user, String password) {
        var hash = hashesByUser.get(user);
        return hash != null && encoder.matches(password, hash);
    }
}
