package jbroker.admin.auth;

import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Login/logout for the UI session plus bearer-token issue/revoke for the
 * REST API. Session fixation is handled by rotating the session id on
 * successful login. Failed logins are audit-logged with the attempted
 * user name (never the password).
 */
@Controller
public class AuthController {

    private static final Logger audit = LoggerFactory.getLogger("jbroker.audit");

    private final AdminUsers users;
    private final TokenStore tokens;

    public AuthController(AdminUsers users, TokenStore tokens) {
        this.users = users;
        this.tokens = tokens;
    }

    @GetMapping("/login")
    public String loginPage(Model model) {
        model.addAttribute("authDisabled", !users.enabled());
        return "login";
    }

    @PostMapping("/login")
    public String login(
            @RequestParam String username, @RequestParam String password, HttpServletRequest req, Model model) {
        if (!users.authenticate(username, password)) {
            audit.info("{} LOGIN_FAILED /login", username);
            model.addAttribute("error", "invalid credentials");
            model.addAttribute("authDisabled", !users.enabled());
            return "login";
        }
        // Create-then-rotate: changeSessionId needs an existing session,
        // and rotating on login blocks session fixation.
        var session = req.getSession(true);
        req.changeSessionId();
        session.setAttribute(AdminAuthFilter.SESSION_PRINCIPAL, username);
        audit.info("{} LOGIN /login", username);
        return "redirect:/";
    }

    @PostMapping("/logout")
    public String logout(HttpServletRequest req) {
        var session = req.getSession(false);
        if (session != null) session.invalidate();
        return "redirect:/login";
    }

    /**
     * Mint a bearer token for the calling operator. Requires an already
     * authenticated request (the filter enforces that), so the principal
     * comes from the session or the presenting token.
     */
    @PostMapping("/api/v1/tokens")
    @ResponseBody
    public ResponseEntity<Map<String, String>> issueToken(HttpServletRequest req) {
        var session = req.getSession(false);
        String principal =
                session != null && session.getAttribute(AdminAuthFilter.SESSION_PRINCIPAL) instanceof String s
                        ? s
                        : null;
        if (principal == null) {
            var header = req.getHeader("Authorization");
            if (header != null && header.startsWith("Bearer ")) {
                principal = tokens.userOf(header.substring("Bearer ".length()).trim())
                        .orElse(null);
            }
        }
        if (principal == null) {
            // Only reachable when auth is disabled; tokens are pointless then.
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "auth_disabled", "message", "configure jbroker.admin.auth.users first"));
        }
        return ResponseEntity.ok(Map.of("token", tokens.issue(principal)));
    }

    /** Revoke a token. Idempotent. */
    @DeleteMapping("/api/v1/tokens/{token}")
    @ResponseBody
    public ResponseEntity<Void> revokeToken(@PathVariable String token) {
        tokens.revoke(token);
        return ResponseEntity.noContent().build();
    }
}
