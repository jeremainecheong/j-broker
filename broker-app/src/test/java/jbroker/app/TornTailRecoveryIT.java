package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import jbroker.broker.ConsumerOffsetsTopic;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tail-integrity recovery (R2.4), end to end: a broker that crashed
 * mid-write comes back serving every intact record and stays writable.
 * The corruption is the worst realistic shape — the last batch has a
 * flipped bit (framing parses, only the CRC catches it) AND garbage bytes
 * follow it (a torn frame). The restart scan must truncate from the
 * corrupt batch, keep everything before it, and continue assigning
 * offsets from the truncation point.
 */
class TornTailRecoveryIT {

    @Test
    void brokerRestartsCleanlyOverACorruptedTail(@TempDir Path dir) throws Exception {
        int raftPort;
        int brokerPort;

        // Session 1: write five records.
        var node = jbroker.app.testkit.TestBrokers.start((rp, bp) -> new Broker.Config(new NodeId(1), dir, rp, bp));
        raftPort = node.raftPort();
        brokerPort = node.brokerPort();
        try (var client = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
            waitReady(node.broker());
            client.createTopic("journal", 1, 1);
            for (int i = 0; i < 5; i++) {
                client.produce("journal", 0, ("r" + i).getBytes());
            }
            assertThat(client.fetchAll("journal", 0, 1 << 20)).hasSize(5);
        } finally {
            node.broker().close();
        }

        // Crash damage: flip a CRC-covered byte in the LAST batch and
        // append a torn frame after it.
        Path logFile;
        try (var stream = Files.list(dir.resolve("topics").resolve("journal-0"))) {
            logFile = stream.filter(p -> p.getFileName().toString().endsWith(".log"))
                    .findFirst()
                    .orElseThrow();
        }
        try (var ch = FileChannel.open(logFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            // Equal payloads make equal-sized batches: frame size is
            // 12 + batchLength from the first header.
            var header = ByteBuffer.allocate(12);
            ch.read(header, 0);
            header.flip();
            header.getLong();
            long frameSize = 12 + header.getInt();
            long lastBatchStart = 4 * frameSize;
            long target = lastBatchStart + 30;
            var one = ByteBuffer.allocate(1);
            ch.read(one, target);
            one.flip();
            ch.write(ByteBuffer.wrap(new byte[] {(byte) (one.get() ^ 0x40)}), target);
            ch.write(ByteBuffer.wrap(new byte[37]), ch.size());
        }

        // Session 2: same dataDir + ports. Recovery drops the corrupt
        // batch and the garbage; the intact prefix serves and the log
        // continues from the truncated offset.
        var broker2 = Broker.start(new Broker.Config(new NodeId(1), dir, raftPort, brokerPort));
        try (var client = new jbroker.broker.client.BrokerClient("127.0.0.1", brokerPort)) {
            waitReady(broker2);
            var survivors = client.fetchAll("journal", 0, 1 << 20);
            assertThat(survivors).hasSize(4);
            assertThat(new String(survivors.get(3))).isEqualTo("r3");

            long assigned = client.produce("journal", 0, "r5".getBytes());
            assertThat(assigned).isEqualTo(4L);
            assertThat(client.fetchAll("journal", 0, 1 << 20)).hasSize(5);
        } finally {
            broker2.close();
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
