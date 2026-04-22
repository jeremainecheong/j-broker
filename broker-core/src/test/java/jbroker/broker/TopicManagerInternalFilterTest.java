package jbroker.broker;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * P7.2 — confirms {@link TopicManager#list()} hides internal topics (name
 * starts with {@code __}) while {@link TopicManager#listAll()} and
 * {@link TopicManager#listInternal()} expose them.
 */
class TopicManagerInternalFilterTest {

    @Test
    void listExcludesInternalTopics() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L, /*internal*/ false, /*compact*/ false);
        tm.onTopicCommitted("__consumer_offsets", 50, 1, 0L, /*internal*/ true, /*compact*/ true);

        assertThat(tm.list()).extracting(TopicDescription::topic).containsExactly("orders");
    }

    @Test
    void listAllIncludesInternalTopics() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L, false, false);
        tm.onTopicCommitted("__consumer_offsets", 50, 1, 0L, true, true);

        assertThat(tm.listAll())
                .extracting(TopicDescription::topic)
                .containsExactlyInAnyOrder("orders", "__consumer_offsets");
    }

    @Test
    void listInternalReturnsOnlyInternals() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L, false, false);
        tm.onTopicCommitted("__consumer_offsets", 50, 1, 0L, true, true);

        assertThat(tm.listInternal()).extracting(TopicDescription::topic).containsExactly("__consumer_offsets");
    }

    @Test
    void topicDescriptionExposesInternalAndCompactFlags() {
        var tm = new TopicManager();
        tm.onTopicCommitted("__consumer_offsets", 50, 3, 123L, true, true);

        var d = tm.describe("__consumer_offsets").orElseThrow();
        assertThat(d.internal()).isTrue();
        assertThat(d.compact()).isTrue();
        assertThat(d.partitions()).isEqualTo(50);
        assertThat(d.replicationFactor()).isEqualTo(3);
        assertThat(d.createdMillis()).isEqualTo(123L);
    }

    @Test
    void backCompatOnTopicCommittedDefaultsInternalAndCompactToFalse() {
        var tm = new TopicManager();
        tm.onTopicCommitted("orders", 1, 1, 0L);

        var d = tm.describe("orders").orElseThrow();
        assertThat(d.internal()).isFalse();
        assertThat(d.compact()).isFalse();
    }
}
