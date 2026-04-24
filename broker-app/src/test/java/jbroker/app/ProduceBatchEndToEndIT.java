package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import jbroker.broker.client.BrokerClient;
import jbroker.raft.core.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Correctness gate for the batched produce path introduced in
 * {@link BrokerClient#produceBatch}. Large-batch throughput is only
 * interesting if it round-trips record-for-record and preserves order;
 * this IT proves both, plus concurrent-writer interleaving safety on a
 * single partition.
 */
class ProduceBatchEndToEndIT {

    @Test
    void producedBatchesAreFetchedBackInOrderAcrossBatchSizes(@TempDir Path dir) throws Exception {
        var broker = startBroker(dir);
        try (var client = new BrokerClient("127.0.0.1", broker.brokerPort())) {
            client.createTopic("bulk", 1, 1);

            // Sweep batch sizes that exercise small/medium/large batching
            // through the same API path. Each batch's records carry their
            // global sequence number as the payload so order is verifiable.
            long totalProduced = 0;
            for (int batchSize : List.of(1, 10, 100, 1_000)) {
                int batches = 5;
                for (int b = 0; b < batches; b++) {
                    var payloads = new ArrayList<byte[]>(batchSize);
                    for (int i = 0; i < batchSize; i++) {
                        long seq = totalProduced + i;
                        payloads.add(Long.toString(seq).getBytes(StandardCharsets.UTF_8));
                    }
                    if (batchSize == 1) client.produce("bulk", 0, payloads.get(0));
                    else client.produceBatch("bulk", 0, payloads);
                    totalProduced += batchSize;
                }
            }

            // Drain and compare byte-for-byte against the expected sequence.
            var fetched = drainPartition(client, "bulk", 0, (int) totalProduced);
            assertThat(fetched)
                    .as("every produced record must round-trip in offset order")
                    .hasSize((int) totalProduced);
            for (int i = 0; i < totalProduced; i++) {
                assertThat(new String(fetched.get(i), StandardCharsets.UTF_8))
                        .as("record at offset %d", i)
                        .isEqualTo(Long.toString(i));
            }
        } finally {
            broker.close();
        }
    }

    @Test
    void concurrentProducersHittingSameBatchedPartitionDontCorruptTheLog(@TempDir Path dir) throws Exception {
        var broker = startBroker(dir);
        try (var client = new BrokerClient("127.0.0.1", broker.brokerPort())) {
            client.createTopic("bulk", 1, 1);

            // 4 concurrent workers, each producing 25 batches of 20 records
            // = 2000 records total. Each worker tags its records with its
            // worker id so we can verify per-worker order without needing
            // a global order (partition offset order is global; the
            // per-worker subsequence must be monotonic because each worker
            // sends sequentially).
            int workers = 4;
            int batchesPerWorker = 25;
            int batchSize = 20;
            int expectedTotal = workers * batchesPerWorker * batchSize;

            try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
                var tasks = new ArrayList<Callable<Void>>();
                for (int w = 0; w < workers; w++) {
                    final int wid = w;
                    tasks.add(() -> {
                        try (var c = new BrokerClient("127.0.0.1", broker.brokerPort())) {
                            int seq = 0;
                            for (int b = 0; b < batchesPerWorker; b++) {
                                var payloads = new ArrayList<byte[]>(batchSize);
                                for (int i = 0; i < batchSize; i++) {
                                    payloads.add(("w" + wid + "-" + (seq++)).getBytes(StandardCharsets.UTF_8));
                                }
                                c.produceBatch("bulk", 0, payloads);
                            }
                        }
                        return null;
                    });
                }
                for (var f : exec.invokeAll(tasks)) f.get();
            }

            var fetched = drainPartition(client, "bulk", 0, expectedTotal);
            assertThat(fetched)
                    .as("the sum of per-worker batches must appear on the partition exactly once")
                    .hasSize(expectedTotal);

            // Per-worker sequences must be intact and monotonic — no lost,
            // reordered, or duplicated records. Partition interleaving
            // between workers is fine and expected.
            var perWorkerNextSeq = new int[workers];
            for (var rec : fetched) {
                String s = new String(rec, StandardCharsets.UTF_8);
                int dash = s.indexOf('-');
                int wid = Integer.parseInt(s.substring(1, dash));
                int seq = Integer.parseInt(s.substring(dash + 1));
                assertThat(seq)
                        .as(
                                "worker %d's record %d arrives in the order that worker produced it",
                                wid, perWorkerNextSeq[wid])
                        .isEqualTo(perWorkerNextSeq[wid]);
                perWorkerNextSeq[wid]++;
            }
            for (int w = 0; w < workers; w++) {
                assertThat(perWorkerNextSeq[w])
                        .as("every record from worker %d must be present", w)
                        .isEqualTo(batchesPerWorker * batchSize);
            }
        } finally {
            broker.close();
        }
    }

    private static List<byte[]> drainPartition(BrokerClient client, String topic, int partition, int expectedTotal) {
        var out = new ArrayList<byte[]>(expectedTotal);
        long offset = 0;
        int stalePolls = 0;
        while (out.size() < expectedTotal && stalePolls < 50) {
            var batch = client.fetch(topic, partition, offset, 4 * 1024 * 1024);
            if (batch.isEmpty()) {
                stalePolls++;
                continue;
            }
            stalePolls = 0;
            out.addAll(batch);
            offset += batch.size();
        }
        return out;
    }

    /**
     * Same pattern as HighVolumeSmokeTest — avoid the TOCTOU bind race the
     * audit's #97 fix documented. 5 attempts with fresh ports is ample.
     */
    private static Broker startBroker(Path dir) throws Exception {
        Exception last = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            int raft = freePort();
            int bp = freePort();
            try {
                return Broker.start(new Broker.Config(new NodeId(1), dir, raft, bp));
            } catch (Exception e) {
                if (!isBindRace(e)) throw e;
                last = e;
            }
        }
        throw new AssertionError("Broker.start hit bind race 5× — real networking issue", last);
    }

    private static boolean isBindRace(Throwable t) {
        while (t != null) {
            if (t instanceof BindException) return true;
            if (t.getClass().getName().contains("NativeIoException")) {
                String m = t.getMessage();
                if (m != null && (m.contains("Address already in use") || m.contains("bind"))) return true;
            }
            t = t.getCause();
        }
        return false;
    }

    private static int freePort() {
        try (var s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
