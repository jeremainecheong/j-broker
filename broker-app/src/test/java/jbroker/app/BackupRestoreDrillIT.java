package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.raft.core.NodeId;
import jbroker.storage.LogVerifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The cold backup/restore drill (R2.6), automated: stop the broker
 * cleanly, copy the whole data dir (partition logs + Raft log +
 * snapshot), verify the copy offline, boot a broker on the copy, and
 * prove every pre-backup committed record serves. This is the procedure
 * an operator runs; the offline check is what tells them the copy is
 * worth restoring before they touch production.
 */
class BackupRestoreDrillIT {

    @Test
    void restoredDataDirServesEveryPreBackupRecord(@TempDir Path original, @TempDir Path backup) throws Exception {
        int raftPort;
        int brokerPort;

        // Live broker: write ten records, then stop cleanly (flushes on close).
        var node = jbroker.app.testkit.TestBrokers.start(
                (rp, bp) -> new Broker.Config(new NodeId(1), original.resolve("data"), rp, bp));
        raftPort = node.raftPort();
        brokerPort = node.brokerPort();
        try (var client = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
            waitReady(node.broker());
            client.createTopic("ledger", 1, 1);
            for (int i = 0; i < 10; i++) {
                client.produce("ledger", 0, ("entry-" + i).getBytes());
            }
            assertThat(client.fetchAll("ledger", 0, 1 << 20)).hasSize(10);
        } finally {
            node.broker().close();
        }

        // Cold copy of the entire data dir.
        var restored = backup.resolve("data");
        copyTree(original.resolve("data"), restored);

        // Offline integrity gate: the backup decodes cleanly end to end.
        var reports = LogVerifier.verify(restored.resolve("topics"));
        assertThat(LogVerifier.allClean(reports))
                .as("backup must verify clean before anyone restores it")
                .isTrue();

        // Restore drill: boot on the copy, same identity and ports.
        var restoredBroker = Broker.start(new Broker.Config(new NodeId(1), restored, raftPort, brokerPort));
        try (var client = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
            waitReady(restoredBroker);
            var records = client.fetchAll("ledger", 0, 1 << 20);
            assertThat(records).hasSize(10);
            assertThat(new String(records.get(9))).isEqualTo("entry-9");

            // The restored cluster is live, not read-only.
            client.produce("ledger", 0, "post-restore".getBytes());
            assertThat(client.fetchAll("ledger", 0, 1 << 20)).hasSize(11);
        } finally {
            restoredBroker.close();
        }
    }

    private static void copyTree(Path from, Path to) throws IOException {
        try (var stream = Files.walk(from)) {
            stream.forEach(src -> {
                try {
                    var dst = to.resolve(from.relativize(src));
                    if (Files.isDirectory(src)) {
                        Files.createDirectories(dst);
                    } else {
                        Files.createDirectories(dst.getParent());
                        Files.copy(src, dst);
                    }
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
        }
    }

    private static void waitReady(Broker broker) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (broker.topics().describe(ConsumerOffsetsTopic.NAME).isPresent()
                    && broker.brokerRegistry().addressFor(1).isPresent()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("broker not ready within 10s");
    }
}
