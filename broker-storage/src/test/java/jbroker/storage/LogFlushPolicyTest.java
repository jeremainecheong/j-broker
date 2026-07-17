package jbroker.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Flush policy (flush.messages / flush.ms). Default is both triggers off —
 * fsync on segment roll plus replication is the durability model — so the
 * policy must be inert unless configured, and each trigger must force the
 * active segment when it fires.
 */
class LogFlushPolicyTest {

    private static final Log.Config BIG_SEGMENTS = new Log.Config(1 << 20, 0, 4096);

    private static void append(Log log, int records) throws Exception {
        for (int i = 0; i < records; i++) {
            log.append(List.of(new Record(0, 0L, null, new byte[16])), 1_000L + i);
        }
    }

    @Test
    void defaultPolicyNeverForces(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, BIG_SEGMENTS)) {
            append(log, 100);
            log.flushIfDue(System.currentTimeMillis() + 10_000_000L);
            assertThat(log.policyFlushCount()).isZero();
        }
    }

    @Test
    void countTriggerForcesEveryNRecords(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, BIG_SEGMENTS)) {
            log.reconfigureFlushPolicy(10, -1);
            append(log, 25);
            assertThat(log.policyFlushCount()).isEqualTo(2L);
            append(log, 5);
            assertThat(log.policyFlushCount()).isEqualTo(3L);
        }
    }

    @Test
    void ageTriggerForcesOnlyWhenUnflushedDataIsOldEnough(@TempDir Path dir) throws Exception {
        try (var log = Log.open(dir, BIG_SEGMENTS)) {
            log.reconfigureFlushPolicy(-1, 1_000);
            append(log, 3);

            log.flushIfDue(System.currentTimeMillis());
            assertThat(log.policyFlushCount()).isZero();

            log.flushIfDue(System.currentTimeMillis() + 5_000);
            assertThat(log.policyFlushCount()).isEqualTo(1L);

            // Nothing unflushed afterwards — the trigger stays quiet.
            log.flushIfDue(System.currentTimeMillis() + 60_000);
            assertThat(log.policyFlushCount()).isEqualTo(1L);
        }
    }

    @Test
    void managerFlushTickDrivesTheAgeTrigger(@TempDir Path dir) throws Exception {
        // Cleaner interval 50ms → flush tick 50ms; flush.ms 100ms resolved
        // per-topic through the resolver, so unflushed data is forced within
        // a few ticks without any produce-side involvement.
        try (var mgr = new LogManager(dir, new LogManager.Config(1 << 20, -1L, -1L, 4096, 50))) {
            mgr.setTopicLogConfigResolver(
                    topic -> Optional.of(new LogManager.TopicLogConfig(1 << 20, -1L, -1L, -1L, 100L)));
            var log = mgr.logFor("orders", 0);
            append(log, 3);

            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline && log.policyFlushCount() == 0) {
                Thread.sleep(20);
            }
            assertThat(log.policyFlushCount()).isGreaterThan(0L);
        }
    }
}
