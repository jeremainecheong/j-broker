package jbroker.bench;

import java.util.Arrays;

/**
 * Entry point for the perf harness. Dispatches on the first
 * argument. The post-release hardening pass added self-contained multi-broker variants:
 *
 * <pre>
 *   j-broker-bench producer --broker HOST:PORT --topic T --partition N
 *                           --records N --payload-size BYTES [--acks all]
 *                           [--csv FILE]
 *
 *   j-broker-bench consumer --broker HOST:PORT --topic T --partition N
 *                           --records N [--max-bytes BYTES] [--csv FILE]
 *
 *   j-broker-bench acks-all    [--records N] [--payload-size B] [--csv FILE]
 *   j-broker-bench replication [--records N] [--payload-size B] [--csv FILE]
 *   j-broker-bench compaction  [--records N] [--keys K]         [--csv FILE]
 * </pre>
 *
 * <p>{@code producer}/{@code consumer} talk to an already-running
 * broker; {@code acks-all}/{@code replication}/{@code compaction} start
 * an in-process cluster themselves, so a single jar invocation is all
 * the operator needs.
 *
 * <p>Every mode prints a final percentile / pause-duration summary plus
 * throughput. Writing to {@code --csv FILE} appends one line per run so
 * multiple calls can aggregate into a single series.
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
            case "acks-all" -> AcksAllPerfTest.run(rest);
            case "replication" -> ReplicationPerfTest.run(rest);
            case "compaction" -> CompactionPausePerfTest.run(rest);
            case "check-batch" -> BatchCorrectness.run(rest);
            case "soak-produce" -> SoakRun.produce(rest);
            case "soak-verify" -> SoakRun.verify(rest);
            default -> {
                usage();
                System.exit(2);
            }
        }
    }

    private static void usage() {
        System.err.println("Usage: j-broker-bench <mode> [flags]");
        System.err.println("  mode: producer | consumer | acks-all | replication | compaction");
        System.err.println("        | soak-produce | soak-verify   (scenario-chaos-with-load.sh)");
        System.err.println("  see class javadoc for per-mode flags");
    }
}
