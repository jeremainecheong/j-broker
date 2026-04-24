package jbroker.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import jbroker.app.Broker;
import jbroker.app.VoterAddress;
import jbroker.raft.core.NodeId;
import jbroker.raft.core.Role;
import jbroker.storage.Log;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * E2E-9-5 at full scale — 1M keyed records across 100 keys → compact →
 * assert count ≈ 100 and wall-clock &lt; 60s on commodity hardware.
 *
 * <p>Tagged {@code slow}: default CI skips; opt into with
 * {@code JBROKER_RUN_SLOW_TESTS=1 ./gradlew test}.
 *
 * <p>Single-broker harness so we're measuring the Log compaction hot path,
 * not cluster-wide replication.
 */
@Tag("slow")
class E2E_9_5_MillionRecordCompactionIT {

    private static final int TOTAL_RECORDS = 1_000_000;
    private static final int DISTINCT_KEYS = 100;

    @Test
    void millionRecordsCompactToDistinctKeysWithinBudget(@TempDir Path dir) throws Exception {
        int brokerPort = freePort();
        int raftPort = freePort();
        var voters = List.of(new VoterAddress(new NodeId(1), "127.0.0.1", raftPort, brokerPort));

        try (var broker = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort, voters))) {
            long deadline = System.currentTimeMillis() + 10_000;
            while (broker.role() != Role.LEADER && System.currentTimeMillis() < deadline) {
                Thread.sleep(25);
            }
            assertThat(broker.role()).isEqualTo(Role.LEADER);

            // Target the broker's internal log directly — skipping the gRPC
            // produce path lets us write 1M records in a reasonable time
            // budget. The compaction logic under test doesn't care how
            // records arrive, only what the log looks like after append.
            Log log = broker.logManager().logFor("ignored", 0);

            long t0 = System.nanoTime();
            for (int i = 0; i < TOTAL_RECORDS; i++) {
                String key = "k" + (i % DISTINCT_KEYS);
                String value = "v" + i;
                log.append(
                        List.of(new jbroker.storage.Record(
                                0, 0L, key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8))),
                        System.currentTimeMillis());
            }
            long appendElapsedMs = (System.nanoTime() - t0) / 1_000_000L;

            long c0 = System.nanoTime();
            int kept = log.compactByKey();
            long compactElapsedMs = (System.nanoTime() - c0) / 1_000_000L;

            assertThat(kept)
                    .as("compacted record count should equal the distinct-key count")
                    .isEqualTo(DISTINCT_KEYS);

            // Hardware budget: 60s covers a laptop SSD + -Xmx1g JVM. Under
            // CI this test will flake on shared runners — that's an
            // acceptable trade-off for the @slow opt-in.
            long totalMs = appendElapsedMs + compactElapsedMs;
            System.out.printf(
                    "E2E-9-5: append %dms + compact %dms = %dms total (kept=%d)%n",
                    appendElapsedMs, compactElapsedMs, totalMs, kept);
            assertThat(totalMs)
                    .as("append + compact should finish under 60s on commodity hardware")
                    .isLessThan(60_000L);
        }
    }

    private static int freePort() {
        try (var sock = new ServerSocket(0)) {
            return sock.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
