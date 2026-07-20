package jbroker.admin.auth;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Session + bearer-token gate in front of every admin surface, plus the
 * audit trail. Applies only when {@link AdminUsers#enabled()}.
 *
 * <ul>
 *   <li>{@code /login}, {@code /logout}, static assets, and
 *       {@code /actuator/*} (health + Prometheus scrape) stay open.
 *   <li>A session established by the login form, or a valid
 *       {@code Authorization: Bearer} token, authenticates the request.
 *   <li>Otherwise: API paths answer {@code 401} JSON; UI paths redirect
 *       to {@code /login}.
 * </ul>
 *
 * <p>Every authenticated mutation (POST/PUT/DELETE/PATCH) is logged to the
 * {@code jbroker.audit} logger as {@code who method path} — the who/what;
 * the log framework stamps the when. Route the logger to a file in
 * production logging config to retain the trail.
 */
@Component
public class AdminAuthFilter extends OncePerRequestFilter {

    /** Session attribute holding the logged-in operator name. */
    public static final String SESSION_PRINCIPAL = "jbroker-admin-principal";

    private static final Logger audit = LoggerFactory.getLogger("jbroker.audit");

    private final AdminUsers users;
    private final TokenStore tokens;

    public AdminAuthFilter(AdminUsers users, TokenStore tokens) {
        this.users = users;
        this.tokens = tokens;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse resp, FilterChain chain)
            throws ServletException, IOException {
        if (!users.enabled() || isOpenPath(req.getRequestURI())) {
            chain.doFilter(req, resp);
            return;
        }

        String principal = authenticatedPrincipal(req);
        if (principal == null) {
            if (req.getRequestURI().startsWith("/api/")) {
                resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                resp.setContentType("application/json");
                resp.getWriter().write("{\"error\":\"unauthorized\",\"message\":\"login or bearer token required\"}");
            } else {
                resp.sendRedirect("/login");
            }
            return;
        }

        if (isMutation(req.getMethod())) {
            audit.info("{} {} {}", principal, req.getMethod(), req.getRequestURI());
        }
        chain.doFilter(req, resp);
    }

    private String authenticatedPrincipal(HttpServletRequest req) {
        var session = req.getSession(false);
        if (session != null && session.getAttribute(SESSION_PRINCIPAL) instanceof String s) {
            return s;
        }
        var header = req.getHeader("Authorization");
        if (header != null && header.startsWith("Bearer ")) {
            return tokens.userOf(header.substring("Bearer ".length()).trim()).orElse(null);
        }
        return null;
    }

    /**
     * Exact-match allowlist. Never suffix rules — {@code endsWith(".js")}
     * would open every API route whose trailing path segment an attacker
     * names (e.g. {@code DELETE /api/v1/topics/orders.js}). Requests whose
     * raw URI smuggles traversal or path parameters are never open either:
     * the container may normalize {@code /vendor/../api/x} into an API
     * route after this check runs.
     */
    private static final java.util.Set<String> OPEN_EXACT = java.util.Set.of(
            "/login", "/logout", "/favicon.ico", "/app.css", "/app.js", "/chaos.js", "/overview.js", "/epoch.js");

    private static boolean isOpenPath(String uri) {
        if (uri.contains("..") || uri.contains(";") || uri.contains("\\")) return false;
        return OPEN_EXACT.contains(uri) || uri.startsWith("/actuator/") || uri.startsWith("/vendor/");
    }

    private static boolean isMutation(String method) {
        return switch (method) {
            case "POST", "PUT", "DELETE", "PATCH" -> true;
            default -> false;
        };
    }
}
