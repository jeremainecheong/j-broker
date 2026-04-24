package jbroker.admin.ui;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import jbroker.admin.api.ClusterController;
import jbroker.admin.api.ConsumerGroupsController;
import jbroker.admin.api.TopicsController;
import jbroker.admin.client.BrokerAdminClient;
import jbroker.admin.client.BrokerAdminClientPool;
import jbroker.admin.dto.ClusterSummary;
import jbroker.admin.dto.NodeInfo;
import jbroker.admin.dto.TopicDetail;
import org.junit.jupiter.api.Test;
import org.springframework.ui.ExtendedModelMap;

/**
 * Unit-level regression coverage for the 2026-04-24 admin-UI audit root
 * cause: Thymeleaf view controllers used to call {@code BrokerAdminClientPool}
 * directly and miss the fan-out + merge that the REST controllers apply.
 * That produced "peer role UNKNOWN" on {@code /} and "HWM / LEO
 * unavailable" alongside a real leader badge on {@code /topics/{name}}.
 *
 * <p>These tests pin the structural fix: the view controllers now delegate
 * to the typed REST-controller methods ({@link ClusterController#cluster()}
 * and {@link TopicsController#describeTopic(String)}), and do not touch the
 * pool's uncached describe paths. Combined with {@code ClusterMergeRolesTest}
 * + {@code TopicsControllerMergeTest} (merge correctness) the whole contract
 * is covered without needing a flaky multi-broker round-trip.
 */
final class ViewControllerDelegationTest {

    @Test
    void overviewViewDelegatesToMergedClusterRestEndpoint() {
        var cluster = mock(ClusterController.class);
        var topics = mock(TopicsController.class);
        var groups = mock(ConsumerGroupsController.class);
        var merged = new ClusterSummary(
                1,
                2L,
                42L,
                List.of(
                        new NodeInfo(1, "h1", 9092, "LEADER", true, 1L),
                        new NodeInfo(2, "h2", 9092, "FOLLOWER", true, 1L),
                        new NodeInfo(3, "h3", 9092, "FOLLOWER", true, 1L)));
        when(cluster.cluster()).thenReturn(merged);
        when(topics.list()).thenReturn(new ArrayList<>());
        when(groups.list()).thenReturn(new ArrayList<>());

        var view = new ClusterViewController(cluster, topics, groups);
        var model = new ExtendedModelMap();
        var tpl = view.index(model);

        assertThat(tpl).isEqualTo("index");
        assertThat(model.getAttribute("cluster")).isSameAs(merged);
        // Every node arrives with a concrete role — the regression would
        // leak UNKNOWNs through if the view called pool.describeCluster
        // instead.
        assertThat(((ClusterSummary) model.getAttribute("cluster")).nodes())
                .extracting(NodeInfo::role)
                .doesNotContain("UNKNOWN");
        verify(cluster).cluster();
    }

    @Test
    void topicDetailViewDelegatesToMergedTopicsRestEndpoint() throws Exception {
        var topicsController = mock(TopicsController.class);
        var pool = mock(BrokerAdminClientPool.class);
        var detail = new TopicDetail("orders", 3, 1, false, false, 0L, Map.of(), List.of());
        when(topicsController.describeTopic("orders")).thenReturn(Optional.of(detail));

        var view = new TopicsViewController(pool, topicsController);
        var model = new ExtendedModelMap();
        var resp = mock(HttpServletResponse.class);
        var tpl = view.detail("orders", model, resp);

        assertThat(tpl).isEqualTo("topic-detail");
        assertThat(model.getAttribute("topic")).isSameAs(detail);
        verify(topicsController).describeTopic("orders");
        // Critical negative check: the view must NOT reach past the REST
        // controller to fetch the un-merged per-broker response. If a
        // future refactor re-introduces pool.firstSuccessful(describe*)
        // here, this assertion fires and the bug doesn't ship.
        verify(pool, never()).firstSuccessful(any());
    }

    @Test
    void topicDetailViewRendersNotFoundWhenRestControllerReturnsEmpty() throws Exception {
        var topicsController = mock(TopicsController.class);
        var pool = mock(BrokerAdminClientPool.class);
        when(topicsController.describeTopic("missing")).thenReturn(Optional.empty());

        var view = new TopicsViewController(pool, topicsController);
        var model = new ExtendedModelMap();
        var resp = mock(HttpServletResponse.class);
        var tpl = view.detail("missing", model, resp);

        assertThat(tpl).isEqualTo("topic-not-found");
        assertThat(model.getAttribute("topicName")).isEqualTo("missing");
        verify(resp).setStatus(404);
    }

    /** Compile-time guard: {@link BrokerAdminClient} stays the pool's value type. */
    @SuppressWarnings("unused")
    private static BrokerAdminClient unusedTypeGuard() {
        return null;
    }
}
