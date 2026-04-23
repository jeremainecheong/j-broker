package jbroker.bench;

import java.util.Arrays;

/**
 * P12.1 — entry point for the perf harness. Dispatches on the first
 * argument into {@link ProducerPerfTest} or {@link ConsumerPerfTest}.
 *
 * <pre>
 *   j-broker-bench producer --broker HOST:PORT --topic T --partition N
 *                           --records N --payload-size BYTES [--acks all]
 *                           [--csv FILE]
 *
 *   j-broker-bench consumer --broker HOST:PORT --topic T --partition N
 *                           --records N [--max-bytes BYTES] [--csv FILE]
 * </pre>
 *
 * <p>Both print a final percentile table (p50 / p99 / p999 / max) plus
 * throughput (records/s and bytes/s). Writing to {@code --csv FILE}
 * appends one line per run so multiple calls can aggregate into a single
 * series for a README table.
 */
public final class PerfMain {

    private PerfMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            System.exit(2);
        }
        var rest = Arrays.copyOfRange(args, 1, args.length);
        switch (args[0]) {
            case "producer" -> ProducerPerfTest.run(rest);
            case "consumer" -> ConsumerPerfTest.run(rest);
            default -> {
                usage();
                System.exit(2);
            }
        }
    }

    private static void usage() {
        System.err.println(
                "Usage: j-broker-bench producer|consumer --broker HOST:PORT --topic T --partition N --records N [...]");
    }
}
