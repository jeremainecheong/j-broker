package jbroker.bench;

import java.util.ArrayList;
import jbroker.broker.client.BrokerClient;

/**
 * audit sidecar — batched-produce correctness check. Sends a known set
 * of records via {@link BrokerClient#produceBatch} then fetches them
 * back and asserts: (a) count matches, (b) bytes match in order.
 *
 * <pre>
 *   j-broker-bench check-batch --broker HOST:PORT --topic T --partition N
 *                              --batches B --batch-size S
 * </pre>
 */
public final class BatchCorrectness {

    private BatchCorrectness() {}

    static void run(String[] args) throws Exception {
        String broker = BenchArgs.get(args, "--broker", "127.0.0.1:9092");
        String topic = BenchArgs.get(args, "--topic", null);
        int partition = BenchArgs.getInt(args, "--partition", 0);
        int batches = BenchArgs.getInt(args, "--batches", 10);
        int batchSize = BenchArgs.getInt(args, "--batch-size", 100);
        if (topic == null) {
            System.err.println("--topic required");
            System.exit(2);
            return;
        }

        int colon = broker.indexOf(':');
        String host = broker.substring(0, colon);
        int port = Integer.parseInt(broker.substring(colon + 1));
        int total = batches * batchSize;

        try (var client = new BrokerClient(host, port)) {
            // Produce `batches` batches of `batchSize` records each; each
            // record's value is its global sequence number as bytes so we
            // can assert order on the read side.
            for (int b = 0; b < batches; b++) {
                var payloads = new ArrayList<byte[]>(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    int seq = b * batchSize + i;
                    payloads.add(Integer.toString(seq).getBytes());
                }
                client.produceBatch(topic, partition, payloads);
            }

            // Drain everything from offset 0 until we stop making progress.
            var fetched = new ArrayList<byte[]>(total);
            long offset = 0;
            int stale = 0;
            while (fetched.size() < total && stale < 50) {
                var got = client.fetch(topic, partition, offset, 1_048_576);
                if (got.isEmpty()) {
                    stale++;
                    continue;
                }
                stale = 0;
                fetched.addAll(got);
                offset += got.size();
            }

            System.out.printf("produced=%d  fetched=%d%n", total, fetched.size());
            if (fetched.size() != total) {
                System.err.printf("COUNT MISMATCH: expected %d, got %d%n", total, fetched.size());
                System.exit(1);
            }
            for (int i = 0; i < total; i++) {
                String got = new String(fetched.get(i));
                String want = Integer.toString(i);
                if (!got.equals(want)) {
                    System.err.printf("ORDER MISMATCH at i=%d: expected '%s' got '%s'%n", i, want, got);
                    System.exit(1);
                }
            }
            System.out.println("OK — " + total + " records round-trip intact and in order");
        }
    }
}
