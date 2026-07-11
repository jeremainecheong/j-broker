package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The cleaner resolves each topic's effective config through the
 * {@link LogManager.TopicLogConfigResolver} — retention limits and
 * segment sizing are per-topic decisions, not one global knob. Topics the
 * resolver doesn't know fall back to the cluster-default {@code Config}.
 */
class LogManagerPerTopicConfigTest {

    private static final long CLEANER_INTERVAL_MS = 50;

    private static LogManager.Config clusterDefaults() {
        // Small segments so tests roll quickly; unlimited retention so only
        // resolver-driven overrides delete anything.
        return new LogManager.Config(200, -1L, -1L, 4096, CLEANER_INTERVAL_MS);
    }

    private static void fill(Log log, int records) throws Exception {
        for (int i = 0; i < records; i++) {
            log.append(List.of(new Record(0, 0L, null, new byte[80])), 1_000L + i);
        }
    }

    private static void awaitSingleSegment(Log log) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (log.segments().size() == 1) return;
            Thread.sleep(20);
        }
        throw new AssertionError("cleaner did not trim to one segment within 5s; have "
                + log.segments().size());
    }

    @Test
    void sizeRetentionAppliesOnlyToTheOverriddenTopic(@TempDir Path dir) throws Exception {
        try (var mgr = new LogManager(dir, clusterDefaults())) {
            mgr.setTopicLogConfigResolver(topic -> "bounded".equals(topic)
                    ? Optional.of(new LogManager.TopicLogConfig(200, -1L, 0L))
                    : Optional.empty());

            var bounded = mgr.logFor("bounded", 0);
            var unbounded = mgr.logFor("unbounded", 0);
            fill(bounded, 20);
            fill(unbounded, 20);
            // Both topics rolled the same way (the cleaner may already have
            // trimmed "bounded", so the stable precondition is on the topic
            // retention must NOT touch).
            int unboundedSegments = unbounded.segments().size();
            assertThat(unboundedSegments).isGreaterThan(1);

            awaitSingleSegment(bounded);

            // The cluster default (unlimited) still governs the other topic.
            assertThat(unbounded.segments()).hasSize(unboundedSegments);
        }
    }

    @Test
    void timeRetentionUsesTheTopicsOwnCutoff(@TempDir Path dir) throws Exception {
        try (var mgr = new LogManager(dir, clusterDefaults())) {
            // Everything below is appended with ~1970 timestamps, so a 1ms
            // retention window expires every closed segment immediately.
            mgr.setTopicLogConfigResolver(topic -> "expiring".equals(topic)
                    ? Optional.of(new LogManager.TopicLogConfig(200, 1L, -1L))
                    : Optional.empty());

            var expiring = mgr.logFor("expiring", 0);
            var keeper = mgr.logFor("keeper", 0);
            fill(expiring, 20);
            fill(keeper, 20);
            int keeperSegments = keeper.segments().size();

            awaitSingleSegment(expiring);

            assertThat(keeper.segments()).hasSize(keeperSegments);
        }
    }

    @Test
    void segmentBytesOverrideReachesAnAlreadyOpenLog(@TempDir Path dir) throws Exception {
        try (var mgr = new LogManager(dir, new LogManager.Config(1 << 20, -1L, -1L, 4096, CLEANER_INTERVAL_MS))) {
            var log = mgr.logFor("orders", 0);
            fill(log, 10);
            assertThat(log.segments()).hasSize(1); // 1 MiB threshold — no rolls

            // Override lands after the log is open, as an UpdateTopicConfig
            // commit would. A cleaner tick pushes it to the live log; keep
            // appending small batches until the 200-byte threshold takes
            // effect. The worst-case total stays far below the original
            // 1 MiB threshold, so a roll can only mean the override landed.
            mgr.setTopicLogConfigResolver(topic -> Optional.of(new LogManager.TopicLogConfig(200, -1L, -1L)));
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline && log.segments().size() == 1) {
                fill(log, 5);
                Thread.sleep(20);
            }
            assertThat(log.segments().size()).isGreaterThan(1);
        }
    }
}
