package jbroker.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import jbroker.app.testkit.TestBrokers;
import jbroker.broker.client.BatchingProducer;
import jbroker.broker.client.BrokerClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end gate for {@link BatchingProducer} against a live single
 * broker. A small batch-size threshold forces many size-triggered flushes
 * across two partitions; every future must resolve to the exact offset its
 * record landed at, and a full read-back must reproduce the per-partition
 * send order byte-for-byte — no gaps, no duplicates. That closes the loop
 * the unit tests fake: the producer's sequence bookkeeping against the
 * broker's real dedup and acks=all commit path.
 */
class BatchingProducerEndToEndIT {

    private static final int RECORDS = 500;
    private static final int PARTITIONS = 2;

    @Test
    void everyFutureResolvesToItsRecordsOffsetAndReadBackMatchesExactly(@TempDir Path dir) throws Exception {
        var broker = TestBrokers.startSingleNode(dir);
        try (var client = new BrokerClient("127.0.0.1", broker.brokerPort())) {
            client.createTopic("stream", PARTITIONS, 1);

            // 512-byte threshold with ~10-byte payloads → dozens of records
            // per batch, many batches per partition. Long linger so size is
            // the dominant trigger; flush() sweeps up the remainders.
            var config = new BatchingProducer.Config(512, /*lingerMs*/ 50, /*deliveryTimeoutMs*/ 120_000, 100);

            var futures = new ArrayList<List<CompletableFuture<Long>>>();
            var expected = new ArrayList<List<String>>();
            for (int p = 0; p < PARTITIONS; p++) {
                futures.add(new ArrayList<>());
                expected.add(new ArrayList<>());
            }

            try (var producer = BatchingProducer.create(client, config)) {
                for (int i = 0; i < RECORDS; i++) {
                    int partition = i % PARTITIONS;
                    String payload = "p" + partition + "-" + (i / PARTITIONS);
                    futures.get(partition)
                            .add(producer.send("stream", partition, payload.getBytes(StandardCharsets.UTF_8)));
                    expected.get(partition).add(payload);
                }
                producer.flush();
            }

            for (int p = 0; p < PARTITIONS; p++) {
                int count = futures.get(p).size();

                // Futures must resolve to contiguous offsets 0..count-1 in
                // send order — the producer is this partition's only writer,
                // so any gap, duplicate, or reordering shows up right here.
                for (int i = 0; i < count; i++) {
                    assertThat(futures.get(p).get(i).get(10, TimeUnit.SECONDS))
                            .as("partition %d record %d", p, i)
                            .isEqualTo((long) i);
                }

                // Read-back must reproduce the send order byte-for-byte.
                var fetched = client.fetchAll("stream", p, 1 << 20);
                assertThat(fetched)
                        .as("partition %d holds each record exactly once", p)
                        .hasSize(count);
                for (int i = 0; i < count; i++) {
                    assertThat(new String(fetched.get(i), StandardCharsets.UTF_8))
                            .as("partition %d offset %d", p, i)
                            .isEqualTo(expected.get(p).get(i));
                }
            }
        } finally {
            broker.close();
        }
    }
}
