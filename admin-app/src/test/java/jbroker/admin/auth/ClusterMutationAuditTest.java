package jbroker.admin.auth;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

/**
 * Cluster mutations are audited like every other admin mutation: the
 * {@code jbroker.audit} logger records {@code who method path} for the
 * authenticated operator, and an anonymous attempt is refused before it
 * can reach the controller — leaving no audit entry, because nothing
 * happened.
 */
final class ClusterMutationAuditTest {

    private final ListAppender<ILoggingEvent> auditEvents = new ListAppender<>();
    private AdminUsers users;
    private TokenStore tokens;
    private AdminAuthFilter filter;

    @BeforeEach
    void setUp() {
        users = new AdminUsers("ops:" + new BCryptPasswordEncoder().encode("pw"));
        tokens = new TokenStore(3600);
        filter = new AdminAuthFilter(users, tokens);
        auditEvents.start();
        auditLogger().addAppender(auditEvents);
    }

    @AfterEach
    void tearDown() {
        auditLogger().detachAppender(auditEvents);
    }

    private static Logger auditLogger() {
        return (Logger) LoggerFactory.getLogger("jbroker.audit");
    }

    @Test
    void authenticatedClusterMutationIsAuditedWithThePrincipal() throws Exception {
        var req = new MockHttpServletRequest("POST", "/api/v1/cluster/rebalance-leadership");
        req.addHeader("Authorization", "Bearer " + tokens.issue("ops"));
        var resp = new MockHttpServletResponse();

        filter.doFilter(req, resp, new MockFilterChain());

        assertThat(resp.getStatus()).isEqualTo(200);
        assertThat(auditEvents.list).anySatisfy(e -> assertThat(e.getFormattedMessage())
                .isEqualTo("ops POST /api/v1/cluster/rebalance-leadership"));
    }

    @Test
    void decommissionIsAuditedWithItsFullPath() throws Exception {
        var req = new MockHttpServletRequest("POST", "/api/v1/cluster/decommission/3");
        req.addHeader("Authorization", "Bearer " + tokens.issue("ops"));

        filter.doFilter(req, new MockHttpServletResponse(), new MockFilterChain());

        assertThat(auditEvents.list).anySatisfy(e -> assertThat(e.getFormattedMessage())
                .isEqualTo("ops POST /api/v1/cluster/decommission/3"));
    }

    @Test
    void anonymousClusterMutationIsRefusedAndLeavesNoAuditEntry() throws Exception {
        var req = new MockHttpServletRequest("POST", "/api/v1/cluster/rebalance-leadership");
        var resp = new MockHttpServletResponse();

        filter.doFilter(req, resp, new MockFilterChain());

        assertThat(resp.getStatus()).isEqualTo(401);
        assertThat(auditEvents.list).isEmpty();
    }
}
